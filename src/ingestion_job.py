from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name

spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------------------------
# 1. BRONZE LAYER: Raw Ingestion using Auto Loader
# -------------------------------------------------------------------------
# Goal: Load raw files as-is from ADLS/S3 and add ingestion metadata.

def ingest_to_bronze(source_path, bronze_table):
    print(f"Ingesting from {source_path} to {bronze_table}...")
    
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")  # or csv, parquet
        .option("cloudFiles.schemaLocation", f"dbfs:/pipelines/checkpoints/{bronze_table}/schema")
        .load(source_path)
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"dbfs:/pipelines/checkpoints/{bronze_table}/data")
        .outputMode("append")
        .table(bronze_table))

# -------------------------------------------------------------------------
# 2. SILVER LAYER: Cleansed & Standardized
# -------------------------------------------------------------------------
# Goal: Deduplicate, handle nulls, and enforce schema.

def transform_to_silver(bronze_table, silver_table):
    print(f"Transforming {bronze_table} to {silver_table}...")
    
    (spark.readStream
        .table(bronze_table)
        .filter(col("user_id").isNotNull())  # Example cleaning logic
        .dropDuplicates(["user_id", "ingested_at"])
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"dbfs:/pipelines/checkpoints/{silver_table}")
        .outputMode("append")
        .table(silver_table))

# -------------------------------------------------------------------------
# 3. GOLD LAYER: Business Ready (Aggregates)
# -------------------------------------------------------------------------
# Goal: High-level metrics for BI (e.g., Daily Active Users).

def aggregate_to_gold(silver_table, gold_table):
    print(f"Aggregating {silver_table} into {gold_table}...")
    
    # Gold often uses 'AvailableNow' for batch-like updates on streams
    (spark.readStream
        .table(silver_table)
        .groupBy("event_date", "region")
        .count()
        .writeStream
        .format("delta")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"dbfs:/pipelines/checkpoints/{gold_table}")
        .outputMode("complete")
        .table(gold_table))

# Execution Logic
if __name__ == "__main__":
    # In a real DAB, these would be passed via job parameters
    RAW_DATA_PATH = "/mnt/datalake/raw/events/"
    
    ingest_to_bronze(RAW_DATA_PATH, "bronze_events")
    transform_to_silver("bronze_events", "silver_events")
    aggregate_to_gold("silver_events", "gold_daily_metrics")