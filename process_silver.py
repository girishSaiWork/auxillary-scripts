from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, split, element_at, 
    to_timestamp, size, array_contains, when, lit, explode
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize Spark (if valid)
# spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
# Update these to match your Bronze output
BRONZE_PATH = "abfss://<container>@<storage_account>.dfs.core.windows.net/bronze/logs_delta"
SILVER_OUTPUT_PATH = "abfss://<container>@<storage_account>.dfs.core.windows.net/silver/logs_enriched"
CHECKPOINT_PATH = "abfss://<container>@<storage_account>.dfs.core.windows.net/silver/_checkpoints/logs_processing"

def process_silver():
    # -------------------------------------------------------------------------
    # READ STREAM FROM BRONZE
    # -------------------------------------------------------------------------
    # We read from the Delta table we created in the previous step
    bronze_df = (spark.readStream
                 .format("delta")
                 .option("ignoreChanges", "true") # Handle updates if files are rewritten/compacted
                 .load(BRONZE_PATH))

    # -------------------------------------------------------------------------
    # TRANSFORMATIONS (Silver Layer)
    # -------------------------------------------------------------------------
    # Goal: Clean, Type-Cast, Enriched
    
    silver_df = bronze_df \
        .withColumn("event_timestamp", to_timestamp(col("eventDateTimestamp"))) \
        .withColumn("event_date", col("event_timestamp").cast("date")) \
        .withColumn("num_fields_viewed", size(col("viewedField"))) \
        .withColumn("app_split", split(col("applicationID"), "-")) \
        .withColumn("platform_type", element_at(col("app_split"), 1)) \
        .withColumn("interface", element_at(col("app_split"), 2)) \
        .withColumn("is_high_risk", 
                    when(array_contains(col("viewedField"), "password") | 
                         array_contains(col("viewedField"), "pin") |
                         array_contains(col("viewedField"), "cvv") |
                         array_contains(col("viewedField"), "ssn"), 
                         lit(True)).otherwise(lit(False))) \
        .drop("app_split", "eventDateTimestamp") # Drop temporary or raw string columns usually

    # -------------------------------------------------------------------------
    # SPLIT STREAMS (Optional Strategy)
    # -------------------------------------------------------------------------
    # Sometimes in Silver you might want to normalize data. 
    # For example, creating a separate stream for "AccessAudit" where we explode the fields.
    # For this script, we will just write the enriched event stream.

    # -------------------------------------------------------------------------
    # WRITE STREAM
    # -------------------------------------------------------------------------
    # Write to Silver Delta Table
    # Partitioning by date is common for efficient querying
    query = (silver_df.writeStream
             .format("delta")
             .outputMode("append")
             .partitionBy("event_date") # Physical data organization
             .option("checkpointLocation", CHECKPOINT_PATH)
             .option("mergeSchema", "true") # Allow schema evolution if needed
             .start(SILVER_OUTPUT_PATH))
    
    print(f"Silver Stream started... Writing to {SILVER_OUTPUT_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    process_silver()
