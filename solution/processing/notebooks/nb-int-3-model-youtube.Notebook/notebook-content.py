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
# - Step 0: Step Up - get variable library, define helper functions
# - Step 1: MODEL Silver Channel table and load to Gold 
# - Step 2: MODEL Silver PlaylistItems table and load to Gold
# - Step 3: MODEL Silver Video Stats table and load to Gold
#  
# This notebook is dynamic: it can be run in Feature workspaces, DEV, TEST and PROD, thanks to the use of Variable libraries (and ABFS paths). 
#  
# #### Step 0: Set-up

# CELL ********************

# import the libraries we need for this notebook
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

# make the variables in the variable library accessible. 
variables = notebookutils.variableLibrary.getLibrary("vl-int-variables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define a few helper functions

def construct_abfs_path(layer="bronze"):
    """Construct base ABFS path for a Lakehouse layer.

    Args:
        layer: One of "bronze", "silver", or "gold".

    Returns:
        str: Full ABFS base path for the specified Lakehouse.
    """
    ws_name = variables.LH_WORKSPACE_NAME

    layer_mapping = {
        "bronze": variables.BRONZE_LH_NAME,
        "silver": variables.SILVER_LH_NAME,
        "gold": variables.GOLD_LH_NAME
    }

    lh_name = layer_mapping.get(layer)
    base_abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/"
    return base_abfs_path


# instantiate the base path for the SILVER Lakehouse 
silver_lh_base_path = construct_abfs_path("silver")

# instantiate the base path for the Gold Lakehouse 
gold_lh_base_path = construct_abfs_path("gold")


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

# #### Step 1: Model Marketing Channels Dataset


# CELL ********************

# get the silver-layer channel_stats table into a Dataframe
df = read_table_to_dataframe(silver_lh_base_path, "youtube", "channel_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# perform the required reshaping of the dataset for the destination
# we're using literals '1' and 'youtube' here as currently that is all this notebook deals with. 
# in the future, we might make this more dynamic. 
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

# MARKDOWN ********************

# About the loading logic: 
# - in this example, we're loading based on the channel_surrogate_id - which is likely to be consistent for all records (unless the client creates a new channel), 
# - and we're also checking for the DATE. 
# - In designing our matching function like this, we're essentially creating an idempotent APPEND statement. We can run our notebook multiple times, but it will only write once per day. 

# CELL ********************

# define the matching fun 
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

# get the silver-layer videos table into a Dataframe

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

# MARKDOWN ********************

# More about the surrogate key strategy & implementation: 
# - with the videos -> assets modelling, the surrogate key requires a bit more upfront work, and then management as we load new records into the Gold table. 
# - we get the max_id from the existing Gold-layer table, and then use that to assign the new records with new surrogate IDs (ordered by Publish Date). 

# CELL ********************

# Get target table for surrogate key management
full_write_path = f"{gold_lh_base_path}Tables/marketing/assets"
delta_table = DeltaTable.forPath(spark, full_write_path)
target_df = delta_table.toDF()

# Get max surrogate ID (same as advanced line 489-491)
max_id = target_df.agg(F.coalesce(F.max("asset_surrogate_id"), F.lit(0))).collect()[0][0]

# Create existing natural_key -> surrogate_key mapping (same as advanced lines 494-498)
existing_lookup = target_df.select(
    F.col("asset_natural_id").alias("_lookup_natural_key"),
    F.col("asset_surrogate_id").alias("_existing_surrogate_id")
)

# Join source with existing lookup (same as advanced lines 504-508)
df_with_existing = source_df.join(
    existing_lookup,
    source_df["asset_natural_id"] == existing_lookup["_lookup_natural_key"],
    "left"
)

# Split into existing and new records (same as advanced lines 511-512)
existing_records = df_with_existing.filter(F.col("_existing_surrogate_id").isNotNull())
new_records = df_with_existing.filter(F.col("_existing_surrogate_id").isNull())

# For existing records: preserve their existing surrogate ID (same as advanced lines 515-517)
existing_with_key = existing_records.withColumn(
    "asset_surrogate_id", F.col("_existing_surrogate_id")
).drop("_lookup_natural_key", "_existing_surrogate_id")

# For new records: generate new surrogate IDs (same as advanced lines 520-524)
if new_records.count() > 0:
    window_spec = Window.orderBy("asset_publish_date")
    new_with_key = new_records.withColumn(
        "asset_surrogate_id", F.row_number().over(window_spec) + max_id
    ).drop("_lookup_natural_key", "_existing_surrogate_id")

    source_with_surrid = existing_with_key.unionByName(new_with_key)
else:
    source_with_surrid = existing_with_key


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# About the loading logic: 
# - in this example, we're loading based on the asset_surrogate_id. This is more of a dimension table, so we don't need new values being written every day, only when it changes.   
# - In designing our matching function like this, we're essentially creating an idempotent APPEND statement. We can run our notebook multiple times, but it will only write once per day. 
# - we specify different writing logic, depending on whether it's a UPDATE or an INSERT. 

# CELL ********************

# MERGE with specific insert
matching_function = """target.asset_surrogate_id = source.asset_surrogate_id""" 

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

# get the silver-layer video_statistics table into a Dataframe
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
