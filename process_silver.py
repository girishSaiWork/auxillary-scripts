from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, split, element_at, 
    to_timestamp, size, array_contains, when, lit, length, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, TimestampType
)
import json

# Initialize Spark
# spark = SparkSession.builder.appName("SilverProcessing").getOrCreate()

# -------------------------------------------------------------------------
# LOAD CONFIGURATION & PATHS
# -------------------------------------------------------------------------
CONFIG_PATH = "e:/Study Space/Analytics Enginerring/Data Engineering/Azure Databricks/Kafka/config.json"
DQ_RULES_PATH = "e:/Study Space/Analytics Enginerring/Data Engineering/Azure Databricks/Kafka/dq_rules.json"

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

with open(DQ_RULES_PATH, 'r') as f:
    dq_config = json.load(f)

base_path = config["storage"]["basePath"]
# Source is Bronze output
BRONZE_PATH = f"{base_path}{config['rawZoneDeltaTableInformation']['path']}"
# Targets
SILVER_OUTPUT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['path']}"
SILVER_ERROR_PATH = f"{base_path}{config['curatedDeltaTableInformation']['errorPath']}"
CHECKPOINT_PATH = f"{base_path}{config['curatedDeltaTableInformation']['checkpoint']}"

def apply_dq_rules(df, rules_config):
    """
    Dynamically applies DQ rules defined in the config.json.
    """
    # Initialize DQ columns
    df_dq = df.withColumn("dq_errors", lit(None).cast(StringType())) \
              .withColumn("is_valid", lit(True))
    
    # -------------------------------------------------------------------------
    # RULE: NOT NULL
    # -------------------------------------------------------------------------
    if "not_null" in rules_config:
        rule = rules_config["not_null"]
        columns = rule.get("columns", [])
        rule_id = rule.get("ruleId", "ERR_NULL")
        
        for c in columns:
            # Check if column exists to avoid analysis errors
            if c in df.columns:
                condition = col(c).isNotNull()
                error_msg = f"{rule_id}: {c} is null"
                
                df_dq = df_dq.withColumn("dq_errors", 
                    when(~condition, concat_ws(", ", col("dq_errors"), lit(error_msg)))
                    .otherwise(col("dq_errors"))
                ).withColumn("is_valid", col("is_valid") & condition)

    # -------------------------------------------------------------------------
    # RULE: REGEX PATTERN
    # -------------------------------------------------------------------------
    if "regex_pattern" in rules_config:
        for check in rules_config["regex_pattern"]:
            c = check["column"]
            pattern = check["pattern"]
            rule_id = check.get("ruleId", "ERR_REGEX")
            
            if c in df.columns:
                condition = col(c).rlike(pattern)
                error_msg = f"{rule_id}: {c} format mismatch"
                
                df_dq = df_dq.withColumn("dq_errors", 
                    when(~condition, concat_ws(", ", col("dq_errors"), lit(error_msg)))
                    .otherwise(col("dq_errors"))
                ).withColumn("is_valid", col("is_valid") & condition)

    # -------------------------------------------------------------------------
    # RULE: ALLOWED VALUES
    # -------------------------------------------------------------------------
    if "allowed_values" in rules_config:
        for check in rules_config["allowed_values"]:
            c = check["column"]
            valid_values = check["values"]
            rule_id = check.get("ruleId", "ERR_INVALID_VALUE")
            
            if c in df.columns:
                condition = col(c).isin(valid_values)
                error_msg = f"{rule_id}: {c} invalid"
                
                df_dq = df_dq.withColumn("dq_errors", 
                    when(~condition, concat_ws(", ", col("dq_errors"), lit(error_msg)))
                    .otherwise(col("dq_errors"))
                ).withColumn("is_valid", col("is_valid") & condition)

    # -------------------------------------------------------------------------
    # RULE: MIN LENGTH
    # -------------------------------------------------------------------------
    if "min_length" in rules_config:
        for check in rules_config["min_length"]:
            c = check["column"]
            min_len = check["length"]
            rule_id = check.get("ruleId", "ERR_LEN")
            
            if c in df.columns:
                condition = length(col(c)) >= min_len
                error_msg = f"{rule_id}: {c} too short"
                
                df_dq = df_dq.withColumn("dq_errors", 
                    when(~condition, concat_ws(", ", col("dq_errors"), lit(error_msg)))
                    .otherwise(col("dq_errors"))
                ).withColumn("is_valid", col("is_valid") & condition)

    # -------------------------------------------------------------------------
    # RULE: ARRAY NOT EMPTY
    # -------------------------------------------------------------------------
    if "array_not_empty" in rules_config:
        rule = rules_config["array_not_empty"]
        columns = rule.get("columns", [])
        rule_id = rule.get("ruleId", "ERR_EMPTY_ARRAY")
        
        for c in columns:
            if c in df.columns:
                condition = size(col(c)) > 0
                error_msg = f"{rule_id}: {c} empty"
                
                df_dq = df_dq.withColumn("dq_errors", 
                    when(~condition, concat_ws(", ", col("dq_errors"), lit(error_msg)))
                    .otherwise(col("dq_errors"))
                ).withColumn("is_valid", col("is_valid") & condition)

    return df_dq

def process_silver():
    # -------------------------------------------------------------------------
    # READ STREAM FROM BRONZE
    # -------------------------------------------------------------------------
    bronze_df = (spark.readStream
                 .format("delta")
                 .option("ignoreChanges", "true") 
                 .load(BRONZE_PATH))

    # -------------------------------------------------------------------------
    # STANDARDIZATION & RENAMING (Based on Config)
    # -------------------------------------------------------------------------
    # We rename columns from Source Name to Target Name as per config
    # And cast types if necessary
    
    transformed_df = bronze_df
    
    for col_def in config["columns"]:
        src = col_def.get("sourceName", col_def["targetName"])
        tgt = col_def["targetName"]
        col_type = col_def["type"]
        
        # Renaissance transformations
        if col_type == "timestamp":
             # Parse string to timestamp
             transformed_df = transformed_df.withColumn(tgt, to_timestamp(col(src)))
        else:
             # Basic rename
             if src != tgt:
                transformed_df = transformed_df.withColumnRenamed(src, tgt)
                
    # -------------------------------------------------------------------------
    # ENRICHMENT (Derived Columns)
    # -------------------------------------------------------------------------
    # Ensure columns exist before using them (post-rename)
    # Example: 'eventTimestamp' is now the column, derived from 'eventDateTimestamp'
    
    # Extract date for partitioning
    transformed_df = transformed_df \
        .withColumn("eventDate", col("eventTimestamp").cast("date"))
        
    # Example enrichment: Parse App ID (now 'applicationCode')
    if "applicationCode" in transformed_df.columns:
         transformed_df = transformed_df \
            .withColumn("app_split", split(col("applicationCode"), "-")) \
            .withColumn("platform_type", element_at(col("app_split"), 1)) \
            .drop("app_split")

    # -------------------------------------------------------------------------
    # APPLY DQ RULES
    # -------------------------------------------------------------------------
    dq_df = apply_dq_rules(transformed_df, dq_config["dataQualityRules"])

    # -------------------------------------------------------------------------
    # WRITE STREAM
    # -------------------------------------------------------------------------
    def write_microbatch(batch_df, batch_id):
        # Cache for double action
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
             .save(SILVER_OUTPUT_PATH))
             
        if error_records.count() > 0:
            (error_records.write
             .format("delta")
             .mode("append")
             .partitionBy("eventDate")
             .option("mergeSchema", "true")
             .save(SILVER_ERROR_PATH))
             
        batch_df.unpersist()

    query = (dq_df.writeStream
             .foreachBatch(write_microbatch)
             .option("checkpointLocation", CHECKPOINT_PATH)
             .start())
    
    print(f"Silver Stream started using Config...")
    print(f"Valid -> {SILVER_OUTPUT_PATH}")
    print(f"Error -> {SILVER_ERROR_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    process_silver()
