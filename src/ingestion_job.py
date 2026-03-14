from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.getOrCreate()

source_path = "/mnt/datalake/raw/events/"
bronze_table = "bronze_events"

query = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"dbfs:/pipelines/checkpoints/{bronze_table}/schema")
        .load(source_path)
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"dbfs:/pipelines/checkpoints/{bronze_table}/data")
        .trigger(availableNow=True)
        .toTable(bronze_table)
)

query.awaitTermination()