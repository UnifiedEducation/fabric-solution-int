# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "ec5eb008-6641-80a2-4ba8-def4c4309668",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## PRJ006 🔶 INT Project, Sprint 6/7 - Running GX on Gold Layer
# 
# > The code in this notebook is written as part of Sprint 6 of the Intermediate Project, in [Fabric Dojo](https://skool.com/fabricdojo/about). The intention is first to get the functionality working, in a way that's understandable for the community. Then, in future weeks, we will layer in things like refactoring, testing, error-handling, more defensive coding patterns to make our cleaning  more robust.
# 
# #### In this notebook:
# - Step 0: Define helper functions, import packages
# - Step 1: Get variables from VLs 
# - Step 2: Copy the GX Context files from the Admin Lakehouse into /tmp/ 
# - Step 3: Initialize the GX Context (and some metadata)
# - Step 4: Run it and log it
# 
# This notebook is dynamic: it can be run in Feature workspaces, DEV, TEST and PROD, thanks to the use of Variable libraries (and ABFS paths). 

# MARKDOWN ********************

# #### Step 0: Define some helper functions


# CELL ********************

import notebookutils
import great_expectations as gx
import shutil
import os
import pandas
from datetime import datetime 
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_df_from_path(base_path, schema, table):
    """Loads data into a dataframe, from the inputs (which come from metadata)"""
    return spark.read.format("delta").load(f"{base_path}/Tables/{schema}/{table}")


def log_results(results: dict, lakehouse_abfs: str) -> None:
    """Append-only logging."""
    
    results_flattened = [
        {
            "validation_id": results.meta['validation_id'],
            "expectation_type": result.expectation_config.type,
            "column": result.expectation_config.kwargs.get("column", None),
            "success": result.success,
            "test_timestamp": datetime.strptime(
                results.meta['batch_markers']['ge_load_time'], 
                "%Y%m%dT%H%M%S.%fZ"
            )
        } for result in results.results
    ]
    
    pandas_df = pd.DataFrame(results_flattened)
    spark_results_df = spark.createDataFrame(pandas_df)
    spark_results_df.write.format("delta").mode("append").save(lakehouse_abfs)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 1: Get variables from variable library


# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-int-variables")
gold_path = f"abfss://{variables.LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{variables.GOLD_LH_NAME}.Lakehouse"
admin_path = f"abfss://{variables.LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{variables.ADMIN_LH_NAME}.Lakehouse"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Copy the GX Context files from the Admin Lakehouse into /tmp/ 
# To get GX to work across multiple deployment environments, we need to use thie technique. 
# Because the `context = gx.get_context(mode="file", project_root_dir=local_path)` requires a local/ relative path - we can't use an ABFS path directly, so we will copy the files locally, and then setup the context from there. 

# CELL ********************

# Clean
local_path = "/tmp/gx_project"
os.makedirs(local_path, exist_ok=True)

# Copy GX context from Admin Lakehouse to local path.
source = f"{admin_path}/Files/gx"
notebookutils.fs.cp(source, f"file://{local_path}/gx", recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Initialize the GX Context (and some metadata)

# CELL ********************

context = gx.get_context(mode="file", project_root_dir=local_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VALIDATION_METADATA = [ 
    {"schema": "marketing", "table": "channels", "validation_definition": "marketing_channels_validation"}, 
    {"schema": "marketing", "table": "assets", "validation_definition": "marketing_assets_validation"}, 
    {"schema": "marketing", "table": "asset_stats", "validation_definition": "marketing_asset_stats_validation"}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 4: Run the validation

# CELL ********************

for item in VALIDATION_METADATA: 
    print(f"Validating: {item['schema']}.{item['table']}")

    # get the validation definition from the GX Context
    validation_def = context.validation_definitions.get(item["validation_definition"])

    # read the data into a Spark dataframe
    df = get_df_from_path(gold_path, item["schema"], item["table"])

    # pass the DF into the validation definition 
    results = validation_def.run(batch_parameters={"dataframe": df})

    log_results(results, f"{admin_path}/Tables/quality/validation_results")

    print(f"100% Success? {results.success}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
