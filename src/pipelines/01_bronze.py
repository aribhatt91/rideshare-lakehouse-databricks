from pyspark import pipelines as dp
from pyspark.sql import functions as F


S3_BUCKET = spark.conf.get("ride_sharing.s3_bucket")

SOURCE_PATH = (
    f"s3://{S3_BUCKET}/eventstream/trips/"
)

@dp.streaming_table(
    name="bronze_trips"
)
def trips():

    return (
        spark.readStream
        .format("cloudFiles")

        # Simulator produces JSONL
        .option("cloudFiles.format", "json")

        # Ask Auto Loader to infer useful Spark data types
        .option("cloudFiles.inferColumnTypes", "true")

        # Automatically add new columns and widen compatible types
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumnsWithTypeWidening"
        )

        # Preserve unexpected fields/type mismatches
        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )

        # Don't drop records because of a type mismatch
        .option(
            "mode",
            "PERMISSIVE"
        )

        .load(SOURCE_PATH)

        # -------------------------------------------------
        # Ingestion metadata
        # -------------------------------------------------

        .withColumn(
            "_ingested_at",
            F.current_timestamp()
        )

        .withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )

        .withColumn(
            "_source_file_modification_time",
            F.col("_metadata.file_modification_time")
        )
    )