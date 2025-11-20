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
import pyspark.sql.functions as F
from pyspark.sql.window import Window


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

transformed_df = df.select(
    F.lit(1).alias("channel_surrogate_id"),
    F.lit("youtube").alias("channel_platform"),
    F.col("channel_name").alias("channel_account_name"),
    F.col("channel_description").alias("channel_account_description"),
    F.col("subscriber_count").alias("channel_total_subscribers"),
    F.col("video_count").alias("channel_total_assets"),
    F.col("view_count").alias("channel_total_views"),
    F.col("loading_TS").alias("modified_TS")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

matching_func = """target.channel_surrogate_id = source.channel_surrogate_id 
            AND to_date(target.modified_TS) = to_date(source.modified_TS)"""
           
write_dataframe_to_table(transformed_df, gold_lh_base_path, "marketing", "channels", matching_func)

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

# perform the base transformation
source_df = df.select(
    F.col("video_id").alias("asset_natural_id"),
    F.lit(1).alias("channel_surrogate_id"),
    F.col("video_title").alias("asset_title"),
    F.col("video_description").alias("asset_text"),
    F.col("video_publish_TS").alias("asset_publish_date"),
    F.col("loading_TS").alias("modified_TS")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get max surrogate ID from target Gold table
full_write_path = f"{gold_lh_base_path}Tables/marketing/assets"

delta_table = DeltaTable.forPath(spark, full_write_path)
target_df = delta_table.toDF()
max_id = target_df.agg(F.coalesce(F.max("asset_surrogate_id"), F.lit(0))).collect()[0][0]

# order the videos/ assets by Publish Date
window_spec = Window.orderBy("asset_publish_date")

# 
source_with_surrid = source_df.withColumn(
    "asset_surrogate_id",
    F.row_number().over(window_spec) + max_id
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MERGE with specific insert
matching_function = """target.asset_surrogate_id = source.asset_surrogate_id 
            AND to_date(target.modified_TS) = to_date(source.modified_TS)""" 

(
    delta_table.alias("target")
    .merge(source_with_surrid.alias("source"), matching_function)
    .whenMatchedUpdate(
        set={
            "asset_title": "source.asset_title",
            "asset_text": "source.asset_text",
            "asset_publish_date": "source.asset_publish_date",
            "modified_TS": "source.modified_TS"
        }
    )
    .whenNotMatchedInsert(
        values={
            "asset_surrogate_id": "source.asset_surrogate_id",
            "asset_natural_id": "source.asset_natural_id",
            "channel_surrogate_id": "source.channel_surrogate_id",
            "asset_title": "source.asset_title",
            "asset_text": "source.asset_text",
            "asset_publish_date": "source.asset_publish_date",
            "modified_TS": "source.modified_TS"
        }
    )
    .execute()
)


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
# Lookup asset_surrogate_id from the assets dimension
assets_df = read_table_to_dataframe(gold_lh_base_path, "marketing", "assets")

asset_lookup = assets_df.select("asset_natural_id", "asset_surrogate_id")

# Join to get asset_surrogate_id, then transform columns
df_transformed = (
    df
    .join(asset_lookup, df.video_id == asset_lookup.asset_natural_id, "left") 
    .select(
        F.col("asset_surrogate_id"),
        F.col("video_view_count").alias("asset_total_views"),
        F.lit(None).alias("asset_total_impressions"),  # empty, where it's unknown. 
        F.col("video_like_count").alias("asset_total_likes"),
        F.col("video_comment_count").alias("asset_total_comments"),
        F.col("loading_TS").alias("modified_TS")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the target table
full_write_path = f"{gold_lh_base_path}Tables/marketing/asset_stats"
delta_table = DeltaTable.forPath(spark, full_write_path)

# MERGE - this is a Type 1 update (overwrite latest stats for each asset)

matching_function = """target.asset_surrogate_id = source.asset_surrogate_id 
            AND to_date(target.modified_TS) = to_date(source.modified_TS)""" 
(
    delta_table.alias("target")
    .merge(df_transformed.alias("source"), matching_function)
    .whenMatchedUpdate(
        set={
            "asset_total_views": "source.asset_total_views",
            "asset_total_impressions": "source.asset_total_impressions",
            "asset_total_likes": "source.asset_total_likes",
            "asset_total_comments": "source.asset_total_comments",
            "modified_TS": "source.modified_TS"
        }
    )
    .whenNotMatchedInsert(
        values={
            "asset_surrogate_id": "source.asset_surrogate_id",
            "asset_total_views": "source.asset_total_views",
            "asset_total_impressions": "source.asset_total_impressions",
            "asset_total_likes": "source.asset_total_likes",
            "asset_total_comments": "source.asset_total_comments",
            "modified_TS": "source.modified_TS"
        }
    )
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
