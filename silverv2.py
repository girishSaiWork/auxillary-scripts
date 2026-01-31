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
    Applies Data Quality rules dynamically based on row values (applicationCode).
    """
    # 1. Setup Error Array Column
    df_dq = df.withColumn("dq_errors", array().cast("array<struct<rule_id:string, column:string, message:string>>")) \
              .withColumn("is_valid", lit(True))
    
    # 2. Iterate through "Metadata Rules"
    for row in policies:
        rule_id = row["policy_id"]
        rule_type = row["rule_type"]
        target_col = row["target_column"]
        params = row["rule_params"]
        msg = row["error_message"]
        
        # Scoping logic
        include_list = row["app_scope_inclusion"]
        exclude_list = row["app_scope_exclusion"]
        
        # 3. Build "Applicability Condition"
        # Rule applies IF:
        #   ( 'ALL' is in inclusion OR appCode is in inclusion )
        #   AND
        #   ( appCode is NOT in exclusion )
        
        # Note: We rely on "applicationCode" existing in the DF
        app_col = col("applicationCode")
        
        is_included = (lit("ALL").isin(include_list)) | (app_col.isin(include_list))
        is_not_excluded = ~(app_col.isin(exclude_list))
        
        should_apply_rule = is_included & is_not_excluded
        
        # 4. Build "Pass Condition" based on Rule Type
        check_condition = lit(True)
        if target_col in df.columns:
            if rule_type == "not_null":
                check_condition = col(target_col).isNotNull()
            elif rule_type == "regex_pattern":
                pattern = params.get("pattern")
                check_condition = col(target_col).rlike(pattern)
            elif rule_type == "array_not_empty":
                check_condition = size(col(target_col)) > 0
            # Add other types as needed
        
        # 5. Apply Logic
        # New Valid State = Current Valid & ( If Should Apply ? Check Condition : True )
        # Meaning: If rule doesn't apply, it's automatically "True" (Pass)
        
        final_rule_logic = when(should_apply_rule, check_condition).otherwise(lit(True))
        
        # 6. Append Error
        # Add error IF (Should Apply AND Check Failed)
        err_struct = struct(
            lit(rule_id).alias("rule_id"), 
            lit(target_col).alias("column"), 
            lit(msg).alias("message")
        )
        
        df_dq = df_dq.withColumn("dq_errors", 
            when(should_apply_rule & (~check_condition), 
                 array_union(col("dq_errors"), array(err_struct))
            ).otherwise(col("dq_errors"))
        ).withColumn("is_valid", col("is_valid") & final_rule_logic)
        
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
    # 2. TRANSFORMATIONS (Metadata Schema Enforcement)
    # -------------------------------------------------------------------------
    # In V3, we trust the SOURCE table schema mostly, but we map explicit columns
    # We load target schema from registry
    target_schema = get_active_schema(DATASET_NAME)
    
    transformed_df = bronze_df
    
    # Simple Mapping: If bronze has 'signInID' and Target Schema has 'signInId', we rename.
    # This assumes target schema field names match the desired output
    # Note: A real mapper might need a separate 'meta_column_mapping' table. 
    # For now, we simulate the previous hardcoded logic but cleaner:
    
    # Rename known variations
    renames = {"signInID": "signInId", "customerID": "customerId", "applicationID": "applicationCode", "eventDateTimestamp": "eventTimestamp", "viewedField": "viewedFieldNames"}
    for src, tgt in renames.items():
        if src in transformed_df.columns:
            transformed_df = transformed_df.withColumnRenamed(src, tgt)
            
    # Cast Timestamp
    if "eventTimestamp" in transformed_df.columns:
        transformed_df = transformed_df.withColumn("eventTimestamp", to_timestamp(col("eventTimestamp")))

    # Enrich Partition Key
    transformed_df = transformed_df \
        .withColumn("eventDate", col("eventTimestamp").cast("date"))
        
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
