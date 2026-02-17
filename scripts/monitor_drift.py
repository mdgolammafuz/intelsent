"""
Daily Drift Monitor.
Target: Postgres (Bronze Layer - 'enriched' table)
Pattern: Batch Monitoring
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import structlog
from sqlalchemy import create_engine

from ml_pipeline.config import MLConfig
from monitoring.drift import DriftDetector
from monitoring.metrics import FEATURE_DRIFT_PSI

# Configure Structured Logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = structlog.get_logger()

def get_postgres_engine(config):
    """
    Create database engine.
    """
    try:
        user = os.environ['POSTGRES_USER']
        password = os.environ['POSTGRES_PASSWORD']
        port = os.environ.get('POSTGRES_PORT', '5432') 
        db_name = os.environ['POSTGRES_DB'] 
        
        # MLConfig reads 'POSTGRES_HOST' from env (which is 'postgres' in docker-compose)
        host = config.postgres_host 

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        return create_engine(db_url)
    except KeyError as e:
        logger.error("missing_credential", var=str(e), msg="Ensure .env is present and loaded")
        sys.exit(1)

def fetch_data(engine, days_back=1, days_window=1):
    """
    Fetch a window of data from bronze.enriched using 'kafka_timestamp'.
    """
    end_date = datetime.now() - timedelta(days=days_back)
    start_date = end_date - timedelta(days=days_window)
    
    query = f"""
    SELECT 
        delay_seconds,
        speed_ms,
        distance_since_last_m
    FROM bronze.enriched
    WHERE kafka_timestamp BETWEEN '{start_date}' AND '{end_date}'
    LIMIT 50000
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error("query_failed", error=str(e))
        return pd.DataFrame()

def main():
    config = MLConfig.from_env()
    
    try:
        engine = get_postgres_engine(config)
        logger.info("drift_check_started", table="bronze.enriched")

        # 1. Fetch Production Data (Yesterday / Last 24h)
        df_current = fetch_data(engine, days_back=0, days_window=1)
        
        if df_current.empty:
            logger.warning("no_data_found", context="current_window_yesterday")
            # Exit gracefully so pipeline continues
            sys.exit(0)

        # 2. Fetch Baseline (Attempt Last Month)
        # In Survival Mode (3-day retention), this will likely return Empty.
        df_baseline = fetch_data(engine, days_back=30, days_window=5)

        # 3. Fallback Logic (Crucial for Survival Mode)
        # If history is deleted, we split current data 50/50 to simulate a check
        if df_baseline.empty or len(df_baseline) < 100:
            logger.warning("baseline_data_missing", msg="Survival Mode detected: Using split of current data as fallback")
            df_baseline = df_current.sample(frac=0.5, random_state=42)

        # 4. Detect Drift
        detector = DriftDetector()
        drift_results = {}
        features_to_check = ["delay_seconds", "speed_ms", "distance_since_last_m"]
        
        for feature in features_to_check:
            curr_values = df_current[feature].dropna().values
            base_values = df_baseline[feature].dropna().values

            if len(curr_values) == 0 or len(base_values) == 0:
                continue

            detector.fit(feature, base_values)
            result = detector.detect(feature, curr_values)
            
            # Update Prometheus Metric
            FEATURE_DRIFT_PSI.labels(feature_name=feature).set(result.psi)
            drift_results[feature] = result.psi
            
            logger.info("feature_checked", 
                        feature=feature, 
                        psi=f"{result.psi:.4f}", 
                        severity=result.severity.value)

        # 5. Save Report
        output_file = "drift.json"
        with open(output_file, "w") as f:
            json.dump(drift_results, f, indent=2)
            
        logger.info("drift_check_complete", output=output_file)

    except Exception as e:
        logger.error("drift_check_critical_failure", error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()