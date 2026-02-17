"""
Gold Aggregation: Silver → Gold (Batch)
Creates aggregated tables for analytics and Feature Store (Postgres).
Methodical: Fails fast if source data or metadata is missing.
Aligned: Forces UTC-to-Helsinki conversion for deterministic contextual features.
Robust: Gracefully handles empty partitions to prevent DB corruption.
"""

import argparse
import logging
import sys
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, dayofweek, hour, when, from_utc_timestamp, trim
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

# Absolute import for package consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def validate_source(spark: SparkSession, path: str, name: str):
    """Checks if a Delta table exists. If not, exits to prevent hangs."""
    if not DeltaTable.isDeltaTable(spark, path):
        logger.critical(f"DEPENDENCY MISSING: {name} table not found at {path}")
        logger.critical(f"Please ensure previous pipeline steps (Silver/Metadata) have completed.")
        sys.exit(1)
    return True


def write_to_postgres(df: DataFrame, table_name: str, config):
    """
    Syncs Gold data to Postgres Marts.
    Methodical: Uses truncate=true to empty the table without dropping it,
    preserving downstream dbt view dependencies.
    """
    # Never truncate and write an empty dataset to Postgres
    if df.count() == 0:
         logger.warning(f"Skipping Postgres sync for {table_name}: Source DataFrame is empty.")
         return

    h_conf = df.sparkSession.sparkContext._jsc.hadoopConfiguration()
    h_conf.set("fs.s3a.access.key", config.minio_access_key)
    h_conf.set("fs.s3a.secret.key", config.minio_secret_key)
    h_conf.set("fs.s3a.endpoint", config.minio_endpoint)
    h_conf.set("fs.s3a.path.style.access", "true")
    h_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    logger.info(f"Syncing {table_name} to Postgres Marts (Truncate Mode)...")
    try:
        df.write \
            .format("jdbc") \
            .option("url", config.postgres_jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config.postgres_user) \
            .option("password", config.postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .option("truncate", "true") \
            .save()
        logger.info(f"Successfully synced {table_name} to Postgres.")
    except Exception as e:
        logger.error(f"Postgres sync failed for {table_name}: {e}")
        raise


def aggregate_daily_metrics(spark: SparkSession, config, target_date: str):
    """Aggregate enriched data into daily KPIs."""
    silver_path = f"{config.silver_path}/enriched"
    gold_path = f"{config.gold_path}/daily_metrics"
    
    validate_source(spark, silver_path, "Silver Enriched")
    
    try:
        # Filter source by the mandatory target date
        silver_df = spark.read.format("delta").load(silver_path).filter(col("date") == target_date)

        # [FIXED] Warn if empty, but proceed to let Spark handle schema consistency
        if silver_df.count() == 0:
            logger.warning(f"No Silver data found for {target_date} in daily_metrics. Propagating schema.")

        # Create delay_category on the fly before aggregation
        processed_df = silver_df.withColumn(
            "delay_category",
            when(col("delay_seconds") <= 60, "on_time")
            .when(col("delay_seconds") <= 300, "minor_delay")
            .otherwise("major_delay")
        )

        daily_df = (
            processed_df.groupBy("line_id", "date")
            .agg(
                count("*").alias("total_events"),
                spark_round(avg("delay_seconds"), 2).alias("avg_delay_seconds"),
                spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                spark_round(avg("door_status"), 4).alias("door_open_rate"),
                spark_sum(when(col("delay_category") == "on_time", 1).otherwise(0)).alias(
                    "on_time_count"
                ),
            )
            .withColumn(
                "on_time_percentage",
                spark_round((col("on_time_count") / col("total_events")) * 100, 2),
            )
        )

        if DeltaTable.isDeltaTable(spark, gold_path):
            dt = DeltaTable.forPath(spark, gold_path)
            dt.alias("t").merge(
                daily_df.alias("s"), "t.line_id = s.line_id AND t.date = s.date"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            daily_df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)

        write_to_postgres(daily_df, "public.fct_daily_performance", config)

    except Exception as e:
        logger.error(f"Daily aggregation failed for {target_date}: {e}")
        sys.exit(1)

def aggregate_hourly_metrics(spark: SparkSession, config, target_date: str):
    """Aggregate enriched data into hourly snapshots with Helsinki context."""
    silver_path = f"{config.silver_path}/enriched"
    gold_path = f"{config.gold_path}/hourly_metrics"
    
    validate_source(spark, silver_path, "Silver Enriched")
    
    try:
        silver_df = spark.read.format("delta").load(silver_path).filter(col("date") == target_date)

        # [FIXED] Propagate empty schema
        if silver_df.count() == 0:
            logger.warning(f"No Silver data found for {target_date} in hourly_metrics. Propagating schema.")

        hourly_df = (
            silver_df.withColumn("local_time", from_utc_timestamp(col("timestamp"), "Europe/Helsinki"))
            .withColumn("hour_val", hour("local_time"))
            .groupBy("line_id", "date", "hour_val")
            .agg(
                count("*").alias("total_events"),
                spark_round(avg("delay_seconds"), 2).alias("avg_delay_seconds"),
                spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            )
            .withColumnRenamed("hour_val", "hour")
        )

        hourly_df.write.format("delta").mode("overwrite").partitionBy("date").save(gold_path)

    except Exception as e:
        logger.error(f"Gold Hourly Aggregation failed: {e}")


def aggregate_stop_performance(spark: SparkSession, config, target_date: str):
    """
    Aggregate stop events for ML feature engineering with coordinate enrichment.
    """
    silver_path = f"{config.silver_path}/stop_events"
    metadata_path = f"{config.gold_path}/stops" 
    gold_path = f"{config.gold_path}/stop_performance"
    
    validate_source(spark, silver_path, "Silver Stop Events")
    validate_source(spark, metadata_path, "Gold Stop Metadata")
    
    try:
        # 1. Load and Cast
        silver_df = spark.read.format("delta").load(silver_path) \
            .filter(col("date") == target_date) \
            .withColumn("stop_id", trim(col("stop_id").cast("string"))) 
            
        # Warn and propagate
        if silver_df.count() == 0:
            logger.warning(f"No Silver data found for {target_date} in stop_performance. Propagating schema.")
            
        stops_metadata = spark.read.format("delta").load(metadata_path) \
            .select(
                trim(col("stop_id").cast("string")).alias("stop_id"),
                col("latitude"),
                col("longitude"),
                col("stop_name") # Added stop_name for the final report
            )

        # 2. Process Metrics (UTC to Helsinki Shift)
        
        stop_metrics = (
            silver_df.withColumn("local_time", from_utc_timestamp(col("arrival_timestamp"), "Europe/Helsinki"))
            .withColumn("hour_of_day", hour("local_time"))
            .withColumn("day_of_week", dayofweek("local_time"))
            .groupBy("stop_id", "line_id", "hour_of_day", "day_of_week")
            .agg(
                count("*").alias("historical_arrival_count"),
                spark_round(avg("delay_at_arrival"), 2).alias("historical_avg_delay"),
                spark_round(avg("dwell_time_ms"), 0).alias("avg_dwell_time_ms")
            )
        )

        # 3. Resilient Join
        enriched_stop_df = stop_metrics.join(stops_metadata, on="stop_id", how="left")

        # 4. Sync
        enriched_stop_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(gold_path)

        write_to_postgres(enriched_stop_df, "public.fct_stop_arrivals", config)
        
        logger.info(f"SUCCESS: {enriched_stop_df.count()} stop arrivals synced for {target_date}.")

    except Exception as e:
        logger.error(f"Stop performance aggregation failed for {target_date}: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=["daily", "hourly", "stops", "all"], default="all")
    # date is now required for OCI/Cron stability
    parser.add_argument("--date", type=str, required=True, help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-GoldAggregation")

    if args.table in ["daily", "all"]:
        aggregate_daily_metrics(spark, config, args.date)
    if args.table in ["hourly", "all"]:
        aggregate_hourly_metrics(spark, config, args.date)
    if args.table in ["stops", "all"]:
        aggregate_stop_performance(spark, config, args.date)

    spark.stop()


if __name__ == "__main__":
    main()