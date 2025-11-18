# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ005 🔶 INT Project (Sprint 5): Data MODEL Notebook (YouTube Datasets)  
#  
# > The code in this notebook is written as part of Week 5 of the Intermediate Project, in [Fabric Dojo](https://skool.com/fabricdojo/about). The intention is first to get the functionality working, in a way that's understandable for the community. Then, in future weeks, we will layer in things like refactoring, testing, error-handling, more defensive coding patterns to make our cleaning  more robust.
# 
# #### In this notebook:
# - Step 0: Solution Step Up - get variable library, define helper functions
# - Step 1: MODEL Silver Channel table and load to Gold 
# - Step 2: MODEL Silver PlaylistItems table and load to Gold
# - Step 3: MODEL Silver Video Stats table and load to Gold
#  
# This notebook is dynamic: it can be run in Feature workspaces, DEV, TEST and PROD, thanks to the use of Variable libraries (and ABFS paths). 
#  
# #### Step 0: Solution set-up

# CELL ********************

import notebookutils 
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-int-variables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define a few helper functions

layer_mapping = {
    "bronze": variables.BRONZE_LH_NAME,
    "silver": variables.SILVER_LH_NAME,
    "gold": variables.GOLD_LH_NAME 
}

def construct_abfs_path(layer = "bronze"): 
    """Constructs a base ABFS path of a Lakehouse, for a given 'layer': ["bronze", "silver", "gold"]
    his can be used to read and write files and tables to/ from a Lakehouse. 
    Reads from the Variable Library. 
    """
    
    ws_name = variables.LH_WORKSPACE_NAME

    lh_name = layer_mapping.get(layer) 

    base_abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/"
    
    return base_abfs_path

silver_lh_base_path = construct_abfs_path("silver")

gold_lh_base_path = construct_abfs_path("gold")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_table_to_dataframe(base_abfs_path, schema, table_name): 
    
    full_path = f"{base_abfs_path}Tables/{schema}/{table_name}"
    
    return spark.read.format("delta").load(full_path)

def write_dataframe_to_table(df, base_abfs_path, schema, table_name, matching_function): 
    
    # add on the full-path to the table
    full_write_path = f"{base_abfs_path}Tables/{schema}/{table_name}"

    delta_table = DeltaTable.forPath(spark, full_write_path)

    (
        delta_table.alias("target")
        .merge(df.alias("source"), matching_function)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll() 
        .execute()
    )
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 1: Model Marketing Channels Dataset


# CELL ********************

df = read_table_to_dataframe(silver_lh_base_path, "youtube", "channel_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# modeling

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_func = """target.channel_surrogate_id = source.channel_surrogate_id 
           AND to_date(target.modified_TS) = to_date(source.modified_TS)"""
           
write_dataframe_to_table(df, gold_lh_base_path, "marketing", "channels", matching_func)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Model Marketing Assets Dataset


# CELL ********************

df = read_table_to_dataframe(silver_lh_base_path, "youtube", "videos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# modeling

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_func = "target.asset_surrogate_id = source.asset_surrogate_id"
           
write_dataframe_to_table(df, gold_lh_base_path, "marketing", "assets", matching_func)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Model Marketing Asset Performance Dataset


# CELL ********************

df = read_table_to_dataframe(silver_lh_base_path, "youtube", "video_statistics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# modeling

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_func = "target.asset_surrogate_id = source.asset_surrogate_id"
           
write_dataframe_to_table(df, gold_lh_base_path, "marketing", "asset_stats", matching_func)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
