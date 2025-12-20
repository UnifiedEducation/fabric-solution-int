# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

status = ""
execution_output = ""
pipeline_id = ""
pipeline_run_id = ""
pipeline_trigger_time = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

import notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-int-variables")


def construct_abfs_path():
    """Construct base ABFS path for the Admin Lakehouse.

    Returns:
        str: Full ABFS base path for the Admin Lakehouse.
    """
    ws_name = variables.LH_WORKSPACE_NAME
    lh_name = variables.ADMIN_LH_NAME
    base_abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/"
    return base_abfs_path



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output = json.loads(execution_output)
parsed_trigger_time = datetime.fromisoformat(pipeline_trigger_time.split(".")[0])

# Build a unified row structure
if status == "Succeeded":
    row_data = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_id": pipeline_id,
        "pipeline_trigger_time": parsed_trigger_time,
        "status": status,
        "execution_duration_seconds": output.get("executionDuration"),
        "error_code": None,
        "error_message": None,
        "failure_type": None,
        "failure_target": None,
        "logged_at_utc": datetime.utcnow().isoformat()
    }
else:
    row_data = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_id": pipeline_id,
        "pipeline_trigger_time": parsed_trigger_time,
        "status": status,
        "execution_duration_seconds": None,
        "error_code": output.get("errorCode"),
        "error_message": output.get("message"),
        "failure_type": output.get("failureType"),
        "failure_target": output.get("target"),
        "logged_at_utc": datetime.utcnow().isoformat()
    }

# Create DataFrame
schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("pipeline_id", StringType(), True),
    StructField("pipeline_trigger_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("execution_duration_seconds", LongType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("failure_type", StringType(), True),
    StructField("failure_target", StringType(), True),
    StructField("logged_at_utc", StringType(), True)
])

df = spark.createDataFrame([row_data], schema)

# Write to Delta table
base_write_path = construct_abfs_path()
full_write_path = f"{base_write_path}Tables/log/pipeline_runs"

if DeltaTable.isDeltaTable(spark, full_write_path):
    delta_table = DeltaTable.forPath(spark, full_write_path)
    (
        delta_table.alias("target")
        .merge(df.alias("source"), "target.pipeline_run_id = source.pipeline_run_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    df.write.format("delta").save(full_write_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
