"""
Silver Transform: Bronze -> Silver (Batch)
Transforms raw Bronze data into cleaned, deduplicated Silver layer.

- Resilient: Uses recursiveFileLookup to handle nested Parquet-as-folder structures.
- Deduplicated: Employs Window functions to keep only the latest offset per event.
- Aligned: Maintains 'timestamp' naming for Gold joins and 'arrival_timestamp' for events.
- Robust: Gracefully handles empty partitions to maintain schema without crashing pipelines.
"""

import argparse
import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when, lit
from pyspark.sql.window import Window

# Absolute import for package consistency
from spark.config import create_spark_session, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def transform_enriched(spark: SparkSession, config, process_date: str):
    """Transform bronze.enriched -> silver.enriched"""
    bronze_path = f"{config.bronze_path}/enriched"
    silver_path = f"{config.silver_path}/enriched"

    try:
        logger.info(f"Reading raw enriched Delta data from {bronze_path}/date={process_date}")
        
        bronze_df = spark.read.format("delta") \
            .load(bronze_path) \
            .filter(col("date") == process_date)
        
        # Do not exit on 0 rows. Let Spark process the empty DataFrame 
        # to initialize/update the Silver Delta table schema for downstream dependencies.
        if bronze_df.count() == 0:
            logger.warning(f"No enriched data found for {process_date}. Processing empty DataFrame to maintain schema.")

        window = Window.partitionBy("vehicle_id", "event_time_ms").orderBy(col("kafka_offset").desc())

        silver_df = (
            bronze_df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num") 
            .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
            # ALIGNMENT: Ensuring types match the expectations of gold_aggregation.py
            .withColumn("door_status", col("door_status").cast("integer"))
            .withColumn("delay_seconds", col("delay_seconds").cast("integer"))
            .withColumn("is_stopped", col("is_stopped").cast("boolean"))
            .withColumn("speed_kmh", col("speed_ms") * 3.6)
            # Preserve 'timestamp' name for Gold Join alignment
            .withColumn("timestamp", 
                when(col("timestamp").isNotNull(), col("timestamp").cast("timestamp"))
                .otherwise((col("event_time_ms") / 1000).cast("timestamp"))
            )
        )

        logger.info(f"Writing {silver_df.count()} deduplicated enriched rows to {silver_path}")
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("date") \
            .save(silver_path)

    except Exception as e:
        logger.error(f"Silver Enriched Transformation failed: {e}")
        sys.exit(1)

def transform_stop_events(spark: SparkSession, config, process_date: str):
    """Transform bronze.stop_events -> silver.stop_events"""
    bronze_path = f"{config.bronze_path}/stop_events"
    silver_path = f"{config.silver_path}/stop_events"

    try:
        logger.info(f"Reading raw stop_events Delta data from {bronze_path}/date={process_date}")
        
        bronze_df = spark.read.format("delta") \
            .load(bronze_path) \
            .filter(col("date") == process_date)
        
        # Handling for stop events.
        if bronze_df.count() == 0:
            logger.warning(f"No stop_events data found for {process_date}. Processing empty DataFrame to maintain schema.")

        window = Window.partitionBy("vehicle_id", "stop_id", "arrival_time").orderBy(
            col("kafka_offset").desc()
        )

        silver_df = (
            bronze_df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            # Required for Gold aggregate_stop_performance joins
            .withColumn("arrival_timestamp", (col("arrival_time") / 1000).cast("timestamp"))
            .withColumn("delay_at_arrival", col("delay_at_arrival").cast("integer"))
            .withColumn("dwell_time_ms", col("dwell_time_ms").cast("long"))
        )

        logger.info(f"Writing {silver_df.count()} deduplicated stop_events to {silver_path}")
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("date") \
            .save(silver_path)

    except Exception as e:
        logger.error(f"Silver Stop Events Transformation failed: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=["enriched", "stop_events", "all"], default="all")
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    config = load_config()
    spark = create_spark_session("TransitFlow-SilverTransform")

    if args.table in ["enriched", "all"]:
        transform_enriched(spark, config, args.date)
    if args.table in ["stop_events", "all"]:
        transform_stop_events(spark, config, args.date)

    spark.stop()

if __name__ == "__main__":
    main()