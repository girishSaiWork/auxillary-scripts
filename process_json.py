from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize Spark Session (implicitly available in Databricks notebooks, but good for standalone scripts)
# spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
# TODO: Update these paths with your actual ADLS Gen2 paths
# Example: "abfss://container_name@storage_account_name.dfs.core.windows.net/path/to/data"
SOURCE_PATH = "abfss://<container>@<storage_account>.dfs.core.windows.net/source_data/stream_data"
BRONZE_OUTPUT_PATH = "abfss://<container>@<storage_account>.dfs.core.windows.net/bronze/logs_delta"
CHECKPOINT_PATH = "abfss://<container>@<storage_account>.dfs.core.windows.net/bronze/_checkpoints/logs_ingestion"

# -------------------------------------------------------------------------
# SCHEMA DEFINITION
# -------------------------------------------------------------------------
# Matches the structure from data_log_gen.py
schema = StructType([
    StructField("signInID", StringType(), True),
    StructField("customerID", StringType(), True),
    StructField("applicationID", StringType(), True),
    StructField("eventDateTimestamp", StringType(), True),
    StructField("viewedField", ArrayType(StringType()), True)
])

def process_stream():
    # -------------------------------------------------------------------------
    # READ STREAM
    # -------------------------------------------------------------------------
    # Using 'multiLine' because the generator produces JSON arrays in each file.
    # Note: For production with millions of small files, consider using Auto Loader ('cloudFiles').
    # To use Auto Loader: .format("cloudFiles").option("cloudFiles.format", "json")
    
    raw_df = (spark.readStream
              .format("json")
              .schema(schema)
              .option("multiLine", "true")  # Required for JSON arrays or pretty-printed JSON
              .option("cleanSource", "archive") # Optional: Archive processed files
              .load(SOURCE_PATH))

    # -------------------------------------------------------------------------
    # TRANSFORMATIONS (Bronze Layer)
    # -------------------------------------------------------------------------
    # Bronze layer typically keeps data raw but adds metadata
    bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("source_filename", input_file_name())

    # -------------------------------------------------------------------------
    # WRITE STREAM
    # -------------------------------------------------------------------------
    # Writing to Delta Lake format is best practice for Databricks
    query = (bronze_df.writeStream
             .format("delta")
             .outputMode("append")
             .option("checkpointLocation", CHECKPOINT_PATH)
             .start(BRONZE_OUTPUT_PATH))
    
    print(f"Stream started... Writing to {BRONZE_OUTPUT_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
