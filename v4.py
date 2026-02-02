from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, split, element_at, 
    to_timestamp, size, array_contains, when, lit, length, 
    concat_ws, array_union, array, struct, expr
)
from pyspark.sql.types import StructType
import json

# Initialize Spark
# spark = SparkSession.builder.appName("SilverProcessing_V3").enableHiveSupport().getOrCreate()

# -------------------------------------------------------------------------
# LOAD METADATA FROM TABLES
# -------------------------------------------------------------------------
# In V3, we load rules/schema from the Delta tables created by metadata_init.py
# rather than config files.

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
    """Fetches DQ rules as a list of dictionaries (Rows collected to driver)"""
    # Collect to driver because rules are typically small (<1000) and we need to iterate
    # to build the column expression tree.
    return spark.table("meta_dq_policy") \
        .filter(col("dataset_name") == dataset_name) \
        .collect()

# -------------------------------------------------------------------------
# CONFIGURATION (Paths & Environment)
# -------------------------------------------------------------------------
# We still use config.json for PATHS, but not for Schema/Rules
CONFIG_PATH = "e:/Study Space/Analytics Enginerring/Data Engineering/Azure Databricks/Kafka/config.json"
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

base_path = config["storage"]["basePath"]
DB_NAME = config["database"]["name"]
TRIGGER_MODE = config.get("trigger", {}).get("type", "availableNow")

BRONZE_TABLE = f"{DB_NAME}.{config['rawZoneDeltaTableInformation']['tableName']}"
DATASET_NAME = "event_log" # logical name valid in metadata tables

SILVER_TABLE_NAME = config['curatedDeltaTableInformation']['tableName']
SILVER_ERROR_TABLE_NAME = f"{SILVER_TABLE_NAME}_error"
SILVER_OUTPUT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['path']}"
SILVER_ERROR_PATH = f"{base_path}{config['curatedDeltaTableInformation']['errorPath']}"
CHECKPOINT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['checkpoint']}"

def apply_dynamic_dq(df, policies):
    """
    Applies Data Quality rules.
    Simplified for readability.
    """
    # Initialize the error accumulator (Array of Error Structs)
    # We start with an empty array.
    df_dq = df.withColumn("dq_errors", array().cast("array<struct<rule_id:string, column:string, message:string>>")) \
              .withColumn("is_valid", lit(True))

    for row in policies:
        # 1. Unpack Metadata
        rule_id = row["policy_id"]
        rule_type = row["rule_type"]
        col_name = row["target_column"]
        msg = row["error_message"]
        
        # 2. Determine Scope (Where does this rule apply?)
        # Logic: (Allowed in ALL OR Current App) AND (Not in Excluded Apps)
        app_code = col("applicationCode")
        scope_ok = (lit("ALL").isin(row["app_scope_inclusion"]) | app_code.isin(row["app_scope_inclusion"])) & \
                   (~app_code.isin(row["app_scope_exclusion"]))

        # 3. Create the Validation Logic (True = Pass, False = Fail)
        # If column doesn't exist, we skip validation (treat as Pass)
        if col_name not in df.columns:
            continue
            
        validation_check = lit(True)
        
        if rule_type == "not_null":
            validation_check = col(col_name).isNotNull()
            
        elif rule_type == "regex_pattern":
            # Keep regex for generic patterns (email, uuid), but users can use 'is_iso_timestamp' for dates
            pattern = row["rule_params"].get("pattern")
            validation_check = col(col_name).rlike(pattern)
            
        elif rule_type == "array_not_empty":
            validation_check = size(col(col_name)) > 0
            
        elif rule_type == "is_iso_timestamp":
            # Native PySpark Check: Try to cast. If NULL result, format is wrong.
            # Format: yyyy-MM-dd'T'HH:mm:ss.SSSXXX (ISO 8601 with Offset)
            # We strictly check strict timestamp parsing
            validation_check = to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").isNotNull()

        # 4. Apply to DataFrame
        # ERROR LOGIC: If (In Scope) AND (Check Failed) -> Add Error
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
        
        # VALID FLAG: If (In Scope) AND (Check Failed) -> Mark Row Invalid
        # We accumulate failures using bitwise AND (True & True = True)
        # If is_error is True, then Valid is False.
        df_dq = df_dq.withColumn("is_valid", col("is_valid") & (~is_error))

    return df_dq

def process_silver():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    # -------------------------------------------------------------------------
    # 1. READ STREAM
    # -------------------------------------------------------------------------
    print(f"Reading from: {BRONZE_TABLE}")
    bronze_df = (spark.readStream
                 .option("ignoreChanges", "true") 
                 .table(BRONZE_TABLE)) 
    
    # -------------------------------------------------------------------------
    # 2. TRANSFORMATIONS (Simplified)
    # -------------------------------------------------------------------------
    # User requested to remove hardcoded renames and casting.
    # We proceed with the columns exactly as they are in Bronze.
    # Note: If DQ rules rely on specific column names (e.g. 'eventDate'), 
    # ensure your Bronze table actually has them.
    
    transformed_df = bronze_df

    # -------------------------------------------------------------------------
    # 3. DYNAMIC DQ APPLICATION
    # -------------------------------------------------------------------------
    policies = get_dq_policies(DATASET_NAME)
    dq_df = apply_dynamic_dq(transformed_df, policies)

    # -------------------------------------------------------------------------
    # 4. WRITE STREAM
    # -------------------------------------------------------------------------
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
             .partitionBy("eventDate")
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
    
    print(f"Silver Stream V3 started with Active Schema & {len(policies)} Policies...")
    query.awaitTermination()

if __name__ == "__main__":
    process_silver()
