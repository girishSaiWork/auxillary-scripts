from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, split, element_at, 
    to_timestamp, size, array_contains, when, lit, length, 
    concat_ws, array_union, array, struct
)
from pyspark.sql.types import StructType
import json

# Initialize Spark
# spark = SparkSession.builder.appName("SilverProcessing_V4").enableHiveSupport().getOrCreate()

# -------------------------------------------------------------------------
# LOAD METADATA HELPERS
# -------------------------------------------------------------------------
def get_active_schema(dataset_name):
    """Fetches the active schema JSON from meta_schema_registry"""
    row = spark.table("meta_schema_registry") \
        .filter((col("dataset_name") == dataset_name) & (col("is_active") == True)) \
        .select("schema_json") \
        .head()
    if row:
        return StructType.fromJson(json.loads(row.schema_json))
    else:
        raise Exception(f"No active schema found for {dataset_name}")

def get_dq_policies(dataset_name):
    """Fetches DQ rules as a list of dictionaries"""
    return spark.table("meta_dq_policy") \
        .filter(col("dataset_name") == dataset_name) \
        .collect()

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
CONFIG_PATH = "e:/Study Space/Analytics Enginerring/Data Engineering/Azure Databricks/Kafka/config.json"
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

base_path = config["storage"]["basePath"]
DB_NAME = config["database"]["name"]
TRIGGER_MODE = config.get("trigger", {}).get("type", "availableNow")

BRONZE_TABLE = f"{DB_NAME}.{config['rawZoneDeltaTableInformation']['tableName']}"
DATASET_NAME = "event_log"

SILVER_TABLE_NAME = config['curatedDeltaTableInformation']['tableName']
SILVER_ERROR_TABLE_NAME = f"{SILVER_TABLE_NAME}_error"
SILVER_OUTPUT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['path']}"
SILVER_ERROR_PATH = f"{base_path}{config['curatedDeltaTableInformation']['errorPath']}"
CHECKPOINT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['checkpoint']}"


# -------------------------------------------------------------------------
# DQ FUNCTION LIBRARY (The "Simpler" Part)
# -------------------------------------------------------------------------
# Each function returns a Column Expression that evaluates to True (Pass) or False (Fail)

def check_not_null(col_name, params):
    return col(col_name).isNotNull()

def check_regex_pattern(col_name, params):
    pattern = params.get("pattern")
    return col(col_name).rlike(pattern)

def check_array_not_empty(col_name, params):
    return size(col(col_name)) > 0

def check_is_iso_timestamp(col_name, params):
    # Strict ISO8601 parsing with Offset
    return to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").isNotNull()

def check_min_length(col_name, params):
    min_len = int(params.get("length"))
    return length(col(col_name)) >= min_len

# Function Registry
# Map 'rule_type' from metadata to actual Python function
DQ_FUNCTIONS = {
    "not_null": check_not_null,
    "regex_pattern": check_regex_pattern,
    "array_not_empty": check_array_not_empty,
    "is_iso_timestamp": check_is_iso_timestamp,
    "min_length": check_min_length
}

def apply_dynamic_dq(df, policies):
    """
    Applies Data Quality rules using the modular function library.
    """
    # 1. Initialize Error Column
    df_dq = df.withColumn("dq_errors", array().cast("array<struct<rule_id:string, column:string, message:string>>")) \
              .withColumn("is_valid", lit(True))

    for row in policies:
        # Unpack Metadata
        rule_id = row["policy_id"]
        rule_type = row["rule_type"]
        col_name = row["target_column"]
        params = row["rule_params"]
        msg = row["error_message"]
        
        # Check if Column Exists
        if col_name not in df.columns:
            continue

        # 2. Get Validation Logic from Library
        if rule_type in DQ_FUNCTIONS:
            # Call the independent function
            dq_func = DQ_FUNCTIONS[rule_type]
            validation_check = dq_func(col_name, params)
        else:
            print(f"Warning: No function defined for rule_type '{rule_type}'. Skipping.")
            continue
            
        # 3. Determine Scope
        app_code = col("applicationCode")
        scope_ok = (lit("ALL").isin(row["app_scope_inclusion"]) | app_code.isin(row["app_scope_inclusion"])) & \
                   (~app_code.isin(row["app_scope_exclusion"]))
        
        # 4. Apply Error Logic
        # Error = (In Scope) AND (Check Failed)
        is_error = scope_ok & (~validation_check)
        
        error_struct = struct(
            lit(rule_id).alias("rule_id"), 
            lit(col_name).alias("column"), 
            lit(msg).alias("message")
        )

        df_dq = df_dq.withColumn("dq_errors", 
            when(is_error, array_union(col("dq_errors"), array(error_struct)))
            .otherwise(col("dq_errors"))
        )
        
        # Valid Flag
        df_dq = df_dq.withColumn("is_valid", col("is_valid") & (~is_error))

    return df_dq

def process_silver():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    # 1. READ STREAM
    print(f"Reading from: {BRONZE_TABLE}")
    bronze_df = (spark.readStream
                 .option("ignoreChanges", "true") 
                 .table(BRONZE_TABLE)) 
    
    # 2. TRANSFORMATIONS (None requested, using Schema as-is)
    transformed_df = bronze_df

    # 3. DYNAMIC DQ APPLICATION
    policies = get_dq_policies(DATASET_NAME)
    dq_df = apply_dynamic_dq(transformed_df, policies)

    # 4. WRITE STREAM
    trigger_opts = {"availableNow": True} if TRIGGER_MODE == "availableNow" else {"processingTime": "5 seconds"}

    def write_microbatch(batch_df, batch_id):
        batch_df.persist()
        
        valid_records = batch_df.filter(col("is_valid") == True).drop("is_valid", "dq_errors")
        error_records = batch_df.filter(col("is_valid") == False)
        
        print(f"Batch {batch_id}: Valid={valid_records.count()}, Error={error_records.count()}")

        if valid_records.count() > 0:
            (valid_records.write
             .format("delta")
             .mode("append")
             .partitionBy("eventDate") # Ensure eventDate exists in Bronze if used here
             .option("mergeSchema", "true")
             .option("path", SILVER_OUTPUT_PATH) 
             .saveAsTable(f"{DB_NAME}.{SILVER_TABLE_NAME}")) 
             
        if error_records.count() > 0:
            (error_records.write
             .format("delta")
             .mode("append")
             .partitionBy("eventDate")
             .option("mergeSchema", "true")
             .option("path", SILVER_ERROR_PATH) 
             .saveAsTable(f"{DB_NAME}.{SILVER_ERROR_TABLE_NAME}"))
             
        batch_df.unpersist()

    query = (dq_df.writeStream
             .foreachBatch(write_microbatch)
             .trigger(**trigger_opts)
             .option("checkpointLocation", CHECKPOINT_PATH)
             .start())
    
    print(f"Silver Stream V4 started...")
    query.awaitTermination()

if __name__ == "__main__":
    process_silver()
