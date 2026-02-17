"""
Maintenance Job: VACUUM, Retention, Optimization
Pattern: Cloud Lifecycle & FinOps (Survival Mode)
Aligned: Uses UTC cutoff to match Delta Lake storage and event telemetry.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
import psycopg2 

from delta.tables import DeltaTable
from spark.config import create_spark_session, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def apply_retention(spark, table_path, retention_days, table_name, base_date: str):
    """
    Purge old partitions from Delta Lake.
    """
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            logger.warning(f"Skipping retention: {table_name} table not found at {table_path}")
            return
            
        base_dt = datetime.strptime(base_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        cutoff_date = (base_dt - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.delete(f"date < '{cutoff_date}'")
        
        logger.info(f"Retention applied to {table_name} (Delta): Removed data older than {cutoff_date}")
    except Exception as e:
        logger.error(f"Retention failed for {table_name}: {e}")

def cleanup_postgres(config, retention_days=3):
    """
    Clean up Postgres Bronze table to prevent disk overflow.
    SURVIVAL MODE: Defaults to 3 days (was 35). 
    We prioritize Disk Space over long-term Drift Monitoring history.
    """
    table_name = "bronze.enriched"
    try:
        # Calculate cutoff (UTC)
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime("%Y-%m-%d")
        
        conn = psycopg2.connect(
            host=config.postgres_host,
            port=config.postgres_port,
            database=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password
        )
        cur = conn.cursor()
        
        # Execute Delete
        query = f"DELETE FROM {table_name} WHERE date < '{cutoff_date}'"
        cur.execute(query)
        deleted_count = cur.rowcount
        conn.commit()
        
        cur.close()
        conn.close()
        
        logger.info(f"Postgres Cleanup: Removed {deleted_count} rows from {table_name} older than {cutoff_date}")
        
    except Exception as e:
        logger.error(f"Postgres cleanup failed: {e}")

def vacuum_table(spark, table_path, table_name, retention_hours=0):
    """
    SURVIVAL MODE: retention_hours=0 removes 'ghost' files immediately.
    """
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            return
        logger.info(f"Vacuuming {table_name} with retention {retention_hours} hours...")
        DeltaTable.forPath(spark, table_path).vacuum(retention_hours)
    except Exception as e:
        logger.error(f"Vacuum failed for {table_name}: {e}")

def optimize_table(spark, table_path, table_name):
    try:
        if not DeltaTable.isDeltaTable(spark, table_path):
            return
        logger.info(f"Optimizing {table_name}...")
        DeltaTable.forPath(spark, table_path).optimize().executeCompaction()
    except Exception as e:
        logger.error(f"Optimize failed for {table_name}: {e}")

def main():
    parser = argparse.ArgumentParser(description="TransitFlow Lakehouse Maintenance")
    parser.add_argument("--action", choices=["retention", "vacuum", "optimize", "all"], default="all")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    parser.add_argument("--date", type=str, required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-Maintenance")
    
    # Disable safety check to allow 0-hour Vacuum (Immediate Deletion)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    tables = {
        "bronze": [
            (f"{config.bronze_path}/enriched", "bronze.enriched", config.bronze_retention_days),
            (f"{config.bronze_path}/stop_events", "bronze.stop_events", config.bronze_retention_days),
        ],
        "silver": [
            (f"{config.silver_path}/enriched", "silver.enriched", config.silver_retention_days),
            (f"{config.silver_path}/stop_events", "silver.stop_events", config.silver_retention_days),
        ],
        "gold": [
            (f"{config.gold_path}/daily_metrics", "gold.daily_metrics", config.gold_retention_days),
            (f"{config.gold_path}/stop_performance", "gold.stop_performance", config.gold_retention_days),
        ],
    }

    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]

    for layer in layers:
        for path, name, days in tables.get(layer, []):
            if args.action in ["retention", "all"]:
                apply_retention(spark, path, days, name, args.date)
            if args.action in ["vacuum", "all"]:
                # Force aggressive vacuum (0 hours) to free disk immediately
                vacuum_table(spark, path, name, retention_hours=0)
            if args.action in ["optimize", "all"]:
                optimize_table(spark, path, name)

    # Run Postgres Cleanup
    if args.action in ["retention", "all"]:
        # 3 days retention for Survival Mode
        cleanup_postgres(config, retention_days=3)

    spark.stop()

if __name__ == "__main__":
    main()