from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, TimestampType, IntegerType, LongType
)
import json

# Initialize Spark Session (if needed for standalone execution)
# spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

# -------------------------------------------------------------------------
# LOAD CONFIGURATION
# -------------------------------------------------------------------------
CONFIG_PATH = "e:/Study Space/Analytics Enginerring/Data Engineering/Azure Databricks/Kafka/config.json"

# In Databricks, if config is in DBFS or Repo, read via regular python open or spark.read
# Here we assume local file access or accessible path.
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

# -------------------------------------------------------------------------
# CONSTRUCT PATHS
# -------------------------------------------------------------------------
base_path = config["storage"]["basePath"]
source_rel_path = config["fileDetails"]["landingZonePath"]
bronze_rel_path = config["rawZoneDeltaTableInformation"]["path"]
checkpoint_rel_path = config["rawZoneDeltaTableInformation"]["checkpoint"]

# Combine base + relative
SOURCE_PATH = f"{base_path}{source_rel_path}"
BRONZE_OUTPUT_PATH = f"{base_path}{bronze_rel_path}"
CHECKPOINT_PATH = f"{base_path}{checkpoint_rel_path}"

# -------------------------------------------------------------------------
# DYNAMIC SCHEMA GENERATION
# -------------------------------------------------------------------------
# We need to map config types to PySpark types
type_mapping = {
    "string": StringType(),
    "integer": IntegerType(),
    "long": LongType(),
    "timestamp": StringType(), # Read as string first from JSON, typically
    "array<string>": ArrayType(StringType())
}

fields = []
# Bronze (Raw) needs to match the SOURCE JSON keys.
# Our config has "sourceName". We use that.
for col_def in config["columns"]:
    src_name = col_def.get("sourceName", col_def["targetName"])
    spark_type = type_mapping.get(col_def["type"], StringType())
    
    # Override: In JSON source, timestamp comes as string usually
    if col_def["type"] == "timestamp":
         spark_type = StringType()
         
    fields.append(StructField(src_name, spark_type, True))

schema = StructType(fields)

def process_stream():
    # -------------------------------------------------------------------------
    # READ STREAM
    # -------------------------------------------------------------------------
    print(f"Reading from: {SOURCE_PATH}")
    
    raw_stream = (spark.readStream
              .format("json")
              .schema(schema)
              .option("multiLine", "true") 
              .load(SOURCE_PATH))

    # -------------------------------------------------------------------------
    # TRANSFORMATIONS (Bronze Layer)
    # -------------------------------------------------------------------------
    bronze_df = raw_stream \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_filename", input_file_name()) \
        .withColumn("ingestion_date", current_timestamp().cast("date"))

    # -------------------------------------------------------------------------
    # WRITE STREAM
    # -------------------------------------------------------------------------
    query = (bronze_df.writeStream
             .format("delta")
             .outputMode(config["rawZoneDeltaTableInformation"]["writeMode"])
             .option("checkpointLocation", CHECKPOINT_PATH)
             # Partitioning by ingestion date helps file management
             .partitionBy("ingestion_date")
             .start(BRONZE_OUTPUT_PATH))
    
    print(f"Stream started... Writing to {BRONZE_OUTPUT_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
