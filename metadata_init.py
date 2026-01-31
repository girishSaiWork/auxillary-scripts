from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, IntegerType, BooleanType, MapType
)
import json

# Initialize Spark
# spark = SparkSession.builder.appName("MetadataSetup").getOrCreate()

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
BASE_PATH = "abfss://<container>@<storage_account>.dfs.core.windows.net/"
# In a real scenario, use actual paths. For this workspace context:
BASE_PATH = "e:/Study Space/Analytics Enginerring/Data Engineering/Azure Databricks/Kafka/metadata/"

SCHEMA_TABLE_PATH = f"{BASE_PATH}meta_schema_registry"
DQ_TABLE_PATH = f"{BASE_PATH}meta_dq_policy"

# -------------------------------------------------------------------------
# 1. SCHEMA REGISTRY DATA MODEL
# -------------------------------------------------------------------------
# Tracks schema versions to ensure strict schema enforcement or evolution
schema_data = [
    {
        "schema_id": "SCH_001",
        "dataset_name": "event_log",
        "version": 1,
        # Storing Spark StructType as JSON string for easy reconstruction
        "schema_json": json.dumps({
            "fields": [
                {"name": "signInId", "type": "string", "nullable": True},
                {"name": "customerId", "type": "string", "nullable": True},
                {"name": "applicationCode", "type": "string", "nullable": True},
                {"name": "eventTimestamp", "type": "string", "nullable": True}, # Stored as string, cast later
                {"name": "viewedFieldNames", "type": {"type": "array", "elementType": "string"}, "nullable": True}
            ]
        }),
        "is_active": True,
        "description": "Initial Schema"
    }
]

schema_schema = StructType([
    StructField("schema_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("version", IntegerType(), False),
    StructField("schema_json", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("description", StringType(), True)
])

# -------------------------------------------------------------------------
# 2. DQ POLICY DATA MODEL
# -------------------------------------------------------------------------
# Parameterized DQ Rules.
# 'app_scope_inclusion': List of AppCodes this apply to. ["ALL"] means everyone.
# 'app_scope_exclusion': List of AppCodes to EXEMPT. (e.g. Mobile doesn't need this check)

dq_data = [
    # 1. Not Null Check (Global)
    {
        "policy_id": "DQ_001",
        "dataset_name": "event_log",
        "rule_type": "not_null",
        "target_column": "signInId",
        "rule_params": {},
        "app_scope_inclusion": ["ALL"],
        "app_scope_exclusion": [],
        "error_message": "signInId is missing"
    },
    # 2. Not Null Check (Specific to Web Portal only)
    # Use Case: Only Web Portal requires viewedFieldNames not be null
    {
        "policy_id": "DQ_002",
        "dataset_name": "event_log",
        "rule_type": "not_null",
        "target_column": "viewedFieldNames",
        "rule_params": {},
        "app_scope_inclusion": ["Web-Banking-Portal"], 
        "app_scope_exclusion": [],
        "error_message": "Web Portal events must have viewed fields"
    },
    # 3. Regex UUID (Global except Legacy ATM)
    # Use Case: ATMs generate old IDs, don't validate them
    {
        "policy_id": "DQ_003",
        "dataset_name": "event_log",
        "rule_type": "regex_pattern",
        "target_column": "signInId",
        "rule_params": {"pattern": r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"},
        "app_scope_inclusion": ["ALL"],
        "app_scope_exclusion": ["ATM-Interface"], 
        "error_message": "Invalid UUID format"
    },
    # 4. array_not_empty (Global)
    {
        "policy_id": "DQ_004",
        "dataset_name": "event_log",
        "rule_type": "array_not_empty",
        "target_column": "viewedFieldNames",
        "rule_params": {},
        "app_scope_inclusion": ["ALL"],
        "app_scope_exclusion": [],
        "error_message": "viewedFieldNames array is empty"
    }
]

dq_schema = StructType([
    StructField("policy_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("rule_type", StringType(), False),
    StructField("target_column", StringType(), False),
    StructField("rule_params", MapType(StringType(), StringType()), True),
    StructField("app_scope_inclusion", ArrayType(StringType()), True),
    StructField("app_scope_exclusion", ArrayType(StringType()), True),
    StructField("error_message", StringType(), True)
])

def setup_metadata():
    print("Creating Metadata Tables...")
    
    # Write Schema Registry
    df_schema = spark.createDataFrame(schema_data, schema=schema_schema)
    (df_schema.write
     .format("delta")
     .mode("overwrite")
     .option("path", SCHEMA_TABLE_PATH)
     .saveAsTable("meta_schema_registry"))
    
    # Write DQ Policies
    df_dq = spark.createDataFrame(dq_data, schema=dq_schema)
    (df_dq.write
     .format("delta")
     .mode("overwrite")
     .option("path", DQ_TABLE_PATH)
     .saveAsTable("meta_dq_policy"))
     
    print(f"Metadata initialized at: {BASE_PATH}")

if __name__ == "__main__":
    setup_metadata()
