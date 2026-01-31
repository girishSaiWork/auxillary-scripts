from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, split, element_at, 
    to_timestamp, size, array_contains, when, lit, length, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, TimestampType
)
import json
import dq_config

# Initialize Spark
# spark = SparkSession.builder.appName("SilverProcessing_V2").enableHiveSupport().getOrCreate()

# -------------------------------------------------------------------------
# LOAD CONFIGURATION
# -------------------------------------------------------------------------
CONFIG_PATH = "e:/Study Space/Analytics Enginerring/Data Engineering/Azure Databricks/Kafka/config.json"

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

base_path = config["storage"]["basePath"]
DB_NAME = config["database"]["name"]
TRIGGER_MODE = config.get("trigger", {}).get("type", "availableNow")

# Source Table (Bronze)
BRONZE_TABLE = f"{DB_NAME}.{config['rawZoneDeltaTableInformation']['tableName']}"

# Target Tables (Silver)
SILVER_TABLE_NAME = config['curatedDeltaTableInformation']['tableName']
SILVER_ERROR_TABLE_NAME = f"{SILVER_TABLE_NAME}_error"

# Paths
SILVER_OUTPUT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['path']}"
SILVER_ERROR_PATH = f"{base_path}{config['curatedDeltaTableInformation']['errorPath']}"
CHECKPOINT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['checkpoint']}"

def apply_dq_rules(df, rules_config):
    """
    Dynamically applies DQ rules defined in the Python Dict.
    Returns dataframe with 'dq_errors' (Array of Error Objects) and 'is_valid' flag.
    """
    from pyspark.sql.functions import struct, array, array_union, col, lit, when, size

    # Initialize DQ columns
    # dq_errors is now an empty ARRAY of structs
    # We create an initial empty array to append to
    df_dq = df.withColumn("dq_errors", array().cast("array<struct<rule_id:string, column:string, message:string>>")) \
              .withColumn("is_valid", lit(True))
    
    def add_error(df, condition, rule_id, column, msg):
        # Create error struct
        error_struct = struct(
            lit(rule_id).alias("rule_id"), 
            lit(column).alias("column"), 
            lit(msg).alias("message")
        )
        
        # If condition fails (~condition), append error struct to array
        # array(error_struct) creates a single-item array to union with the main array
        return df.withColumn("dq_errors", 
            when(~condition, 
                 array_union(col("dq_errors"), array(error_struct))
            ).otherwise(col("dq_errors"))
        ).withColumn("is_valid", col("is_valid") & condition)

    # -------------------------------------------------------------------------
    # RULE: NOT NULL
    # -------------------------------------------------------------------------
    if "not_null" in rules_config:
        rule = rules_config["not_null"]
        columns = rule.get("columns", [])
        rule_id = rule.get("rule_id", "ERR_NULL")
        
        for c in columns:
            if c in df.columns:
                df_dq = add_error(df_dq, col(c).isNotNull(), rule_id, c, "Value is Null")

    # -------------------------------------------------------------------------
    # RULE: REGEX PATTERN
    # -------------------------------------------------------------------------
    if "regex_pattern" in rules_config:
        for check in rules_config["regex_pattern"]:
            c = check["column"]
            pattern = check["pattern"]
            rule_id = check.get("rule_id", "ERR_REGEX")
            
            if c in df.columns:
                df_dq = add_error(df_dq, col(c).rlike(pattern), rule_id, c, f"Pattern match failed: {pattern}")

    # -------------------------------------------------------------------------
    # RULE: ALLOWED VALUES
    # -------------------------------------------------------------------------
    if "allowed_values" in rules_config:
        for check in rules_config["allowed_values"]:
            c = check["column"]
            valid_values = check["values"]
            rule_id = check.get("rule_id", "ERR_INVALID_VALUE")
            
            if c in df.columns:
                df_dq = add_error(df_dq, col(c).isin(valid_values), rule_id, c, "Value not in allowed list")

    # -------------------------------------------------------------------------
    # RULE: MIN LENGTH
    # -------------------------------------------------------------------------
    if "min_length" in rules_config:
        for check in rules_config["min_length"]:
            c = check["column"]
            min_len = check["length"]
            rule_id = check.get("rule_id", "ERR_LEN")
            
            if c in df.columns:
                df_dq = add_error(df_dq, length(col(c)) >= min_len, rule_id, c, f"Length < {min_len}")

    # -------------------------------------------------------------------------
    # RULE: ARRAY NOT EMPTY
    # -------------------------------------------------------------------------
    if "array_not_empty" in rules_config:
        rule = rules_config["array_not_empty"]
        columns = rule.get("columns", [])
        rule_id = rule.get("rule_id", "ERR_EMPTY_ARRAY")
        
        for c in columns:
            if c in df.columns:
                df_dq = add_error(df_dq, size(col(c)) > 0, rule_id, c, "Array is empty")

    return df_dq

def process_silver():
    # Setup Database (Just in case Silver runs independently)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")

    # -------------------------------------------------------------------------
    # READ STREAM FROM TABLE
    # -------------------------------------------------------------------------
    print(f"Reading from Table: {BRONZE_TABLE}")
    bronze_df = (spark.readStream
                 .option("ignoreChanges", "true") 
                 .table(BRONZE_TABLE)) # Access via Metastore

    # -------------------------------------------------------------------------
    # TRANSFORMATIONS
    # -------------------------------------------------------------------------
    transformed_df = bronze_df
    
    for col_def in config["columns"]:
        src = col_def.get("sourceName", col_def["targetName"])
        tgt = col_def["targetName"]
        col_type = col_def["type"]
        
        if col_type == "timestamp":
             transformed_df = transformed_df.withColumn(tgt, to_timestamp(col(src)))
        elif src != tgt:
            transformed_df = transformed_df.withColumnRenamed(src, tgt)

    transformed_df = transformed_df \
        .withColumn("eventDate", col("eventTimestamp").cast("date"))
        
    if "applicationCode" in transformed_df.columns:
         transformed_df = transformed_df \
            .withColumn("app_split", split(col("applicationCode"), "-")) \
            .withColumn("platform_type", element_at(col("app_split"), 1)) \
            .drop("app_split")

    # -------------------------------------------------------------------------
    # APPLY DQ RULES
    # -------------------------------------------------------------------------
    dq_df = apply_dq_rules(transformed_df, dq_config.DQ_RULES)

    # -------------------------------------------------------------------------
    # WRITE STREAM
    # -------------------------------------------------------------------------
    trigger_opts = {"availableNow": True} if TRIGGER_MODE == "availableNow" else {"processingTime": "5 seconds"}

    def write_microbatch(batch_df, batch_id):
        # We need to act on the batch, so persist matches logic
        batch_df.persist()
        
        valid_records = batch_df.filter(col("is_valid") == True).drop("is_valid", "dq_errors")
        error_records = batch_df.filter(col("is_valid") == False)
        
        print(f"Batch {batch_id}: Valid={valid_records.count()}, Error={error_records.count()}")

        # WRITE VALID TO TABLE (External)
        if valid_records.count() > 0:
            (valid_records.write
             .format("delta")
             .mode("append")
             .partitionBy("eventDate")
             .option("mergeSchema", "true")
             .option("path", SILVER_OUTPUT_PATH) # External Location
             .saveAsTable(f"{DB_NAME}.{SILVER_TABLE_NAME}")) # Register in Metastore
             
        # WRITE ERROR TO TABLE (External)
        if error_records.count() > 0:
            (error_records.write
             .format("delta")
             .mode("append")
             .partitionBy("eventDate")
             .option("mergeSchema", "true")
             .option("path", SILVER_ERROR_PATH) # External Location
             .saveAsTable(f"{DB_NAME}.{SILVER_ERROR_TABLE_NAME}"))
             
        batch_df.unpersist()

    query = (dq_df.writeStream
             .foreachBatch(write_microbatch)
             .trigger(**trigger_opts)
             .option("checkpointLocation", CHECKPOINT_PATH)
             .start())
    
    print(f"Silver Stream started with trigger: {TRIGGER_MODE}")
    print(f"Valid Table -> {DB_NAME}.{SILVER_TABLE_NAME}")
    print(f"Error Table -> {DB_NAME}.{SILVER_ERROR_TABLE_NAME}")
    
    query.awaitTermination()

if __name__ == "__main__":
    process_silver()
