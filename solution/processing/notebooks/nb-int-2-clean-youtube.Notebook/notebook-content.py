# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ005 🔶 INT Project (Sprint 5): Data CLEAN Notebook (YouTube Datasets)  
#  
# > The code in this notebook is written as part of Week 5 of the Intermediate Project, in [Fabric Dojo](https://skool.com/fabricdojo/about). The intention is first to get the functionality working, in a way that's understandable for the community. Then, in future weeks, we will layer in things like refactoring, testing, error-handling, more defensive coding patterns to make our cleaning  more robust.
# 
# #### In this notebook:
# - Step 0: Solution Step Up - get variable library, define helper functions
# - Step 1: Clean Bronze Channel table and load to Silver
# - Step 2: Clean Bronze PlaylistItems table and load to Silver
# - Step 3: Clean Bronze Video Stats table and load to Silver
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

def construct_abfs_path(layer="bronze"):
    """Construct base ABFS path for a Lakehouse layer.

    Args:
        layer: One of "bronze", "silver", or "gold".

    Returns:
        str: Full ABFS base path for the specified Lakehouse.
    """
    ws_name = variables.LH_WORKSPACE_NAME
    lh_name = layer_mapping.get(layer)
    base_abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/"
    return base_abfs_path

bronze_lh_base_path = construct_abfs_path("bronze")

silver_lh_base_path = construct_abfs_path("silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_table_to_dataframe(base_abfs_path, schema, table_name):
    """Read a Delta table into a Spark DataFrame.

    Args:
        base_abfs_path: Base ABFS path for the Lakehouse.
        schema: Schema name containing the table.
        table_name: Name of the table to read.

    Returns:
        DataFrame: Spark DataFrame with the table contents.
    """
    full_path = f"{base_abfs_path}Tables/{schema}/{table_name}"
    return spark.read.format("delta").load(full_path)


def write_dataframe_to_table(df, base_abfs_path, schema, table_name, matching_function):
    """Merge a DataFrame into an existing Delta table.

    Args:
        df: Source Spark DataFrame to merge.
        base_abfs_path: Base ABFS path for the Lakehouse.
        schema: Schema name containing the table.
        table_name: Target table name.
        matching_function: SQL expression for merge matching.
    """
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

# #### Step 1: Cleaning Channel Dataset
# Cleaning strategy for this dataset: 
# - filtering of rows which have a blank unique ID column 
# - de-duplication using a Window function


# CELL ********************

df = read_table_to_dataframe(bronze_lh_base_path, "youtube", "channel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Remove nulls in the channel ID column
df_clean = df.filter(F.col("channel_id").isNotNull())

# create a window (that we'll use for deduplication)
window_spec = Window.partitionBy("channel_id", F.to_date("loading_TS")).orderBy(F.col("loading_TS").desc())

df_clean = (df_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_func = """target.channel_id = source.channel_id 
           AND to_date(target.loading_TS) = to_date(source.loading_TS)"""
           
write_dataframe_to_table(df_clean, silver_lh_base_path, "youtube", "channel_stats", matching_func)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Cleaning PlaylistItems dataset
# Cleaning strategy for this dataset: 
# - filtering of rows which have a blank unique ID column 
# - de-duplication using a Window function


# CELL ********************

df = read_table_to_dataframe(bronze_lh_base_path, "youtube", "playlist_items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# do some cleaning
# Remove nulls in the video ID and video Title column
df_clean = df.filter(
    F.col("video_id").isNotNull() & 
    F.col("video_title").isNotNull()
)

# create a window (that we'll use for deduplication)
window_spec = Window.partitionBy("video_id").orderBy(F.col("loading_TS").desc())

df_clean = (
    df_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_func = "target.video_id = source.video_id"
           
write_dataframe_to_table(df_clean, silver_lh_base_path, "youtube", "videos", matching_func)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Cleaning Video Stats dataset
# Cleaning strategy for this dataset: 
# - filtering of rows which have a blank unique ID column 
# - de-duplication using a Window function

# CELL ********************

df = read_table_to_dataframe(bronze_lh_base_path, "youtube", "videos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# do some cleaning
df_clean = df.filter(F.col("video_id").isNotNull())

# create a window (that we'll use for deduplication)
window_spec = Window.partitionBy("video_id", F.to_date("loading_TS")).orderBy(F.col("loading_TS").desc())

df_clean = (
    df_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_func = "target.video_id = source.video_id"

write_dataframe_to_table(df_clean, silver_lh_base_path, "youtube", "video_statistics", matching_func)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
