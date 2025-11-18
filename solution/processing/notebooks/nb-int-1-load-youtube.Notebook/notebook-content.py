# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ005 🔶 INT Project (Sprint 5): Data LOAD Notebook (YouTube Datasets)  
#  
# > The code in this notebook is written as part of Week 5 of the Intermediate Project, in [Fabric Dojo](https://skool.com/fabricdojo/about). The intention is first to get the functionality working, in a way that's understandable for the community. Then, in future weeks, we will layer in things like refactoring, testing, error-handling, more defensive coding patterns to make our loading more robust.
# 
# #### In this notebook:
# - Step 0: Solution Step Up - get variable library, define helper functions
# - Step 1: Raw Channel JSON data to table
# - Step 2: Raw Playlist Items JSON data to table
# - Step 3: Raw Video Statistics JSON data to table
#  
# This notebook is dynamic: it can be run in Feature workspaces, DEV, TEST and PROD, thanks to the use of Variable libraries (and ABFS paths). 
#  


# CELL ********************

import notebookutils 
from delta.tables import DeltaTable
from pyspark.sql.functions import explode, col, current_timestamp

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

def construct_abfs_path(): 
    """Constructs a base ABFS path of a Lakehouse. This can be used to read and writes 
    files and tables to/ from the Lakehouse. 
    Reads from the Variable Library. 
    """
    
    # 'variables' is the values from the Variable Library 
    ws_name = variables.LH_WORKSPACE_NAME
    lh_name = variables.BRONZE_LH_NAME

    base_abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/"
    
    return base_abfs_path
 
def get_most_recent_file(channel_path): 
    base_abfs_path = construct_abfs_path() 
    full_file_path = f"{base_abfs_path}{channel_path}"
    files = notebookutils.fs.ls(full_file_path)
    most_recent_file = max(files, key=lambda file: file.modifyTime)
    return most_recent_file 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Raw Channel JSON data to table

# CELL ********************

channel_path =  "Files/youtube_data_v3/channels/"

most_recent_file = get_most_recent_file(channel_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_channel_json_to_df(most_recent_file): 

    # read raw json into raw_df 
    raw_df = spark.read.option("multiLine", "true").json(most_recent_file.path)

    #explore the items array
    df_channels = raw_df.select(explode(col("items")).alias("item"))

    # cherrypick the properties we need
    df_final = df_channels.select(
        col("item.id").alias("channel_id"),
        col("item.snippet.title").alias("channel_name"),
        col("item.snippet.description").alias("channel_description"),
        col("item.statistics.viewCount").cast("int").alias("view_count"),
        col("item.statistics.subscriberCount").cast("int").alias("subscriber_count"),
        col("item.statistics.videoCount").cast("int").alias("video_count"),
        current_timestamp().alias("loading_TS")
    )

    return df_final

bronze_df_channel = flatten_channel_json_to_df(most_recent_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get the lakehouse abfs path
base_write_path = construct_abfs_path() 

# add on the full-path to the table
full_write_path = f"{base_write_path}Tables/youtube/channel"

delta_table = DeltaTable.forPath(spark, full_write_path)
    
(
    delta_table.alias("target")
    .merge(bronze_df_channel.alias("source"),
        """target.channel_id = source.channel_id 
           AND to_date(target.loading_TS) = to_date(source.loading_TS)"""
    )
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

# ## Step 2: Raw Playlist Items JSON data to table

# CELL ********************

channel_path =  "Files/youtube_data_v3/playlistItems/"

most_recent_file = get_most_recent_file(channel_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_playlistItems_json_to_df(most_recent_file): 

    # read raw json into raw_df 
    raw_df = spark.read.option("multiLine", "true").json(most_recent_file.path)

    # Select and flatten to your desired structure
    df_final = raw_df.select(
        col("snippet.channelId").alias("channel_id"),
        col("snippet.resourceId.videoId").alias("video_id"),
        col("snippet.title").alias("video_title"),
        col("snippet.description").alias("video_description"),
        col("snippet.thumbnails.high.url").alias("thumbnail_url"),
        to_timestamp(col("snippet.publishedAt")).alias("video_publish_TS"),
        current_timestamp().alias("loading_TS")
    )

    return df_final

bronze_df_playlist_items = flatten_playlistItems_json_to_df(most_recent_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get the lakehouse abfs path
base_write_path = construct_abfs_path() 

# add on the full-path to the table
full_write_path = f"{base_write_path}Tables/youtube/playlist_items"

delta_table = DeltaTable.forPath(spark, full_write_path)

# this time, because we have all the data in our df, we will use MERGE. 
( 
    delta_table.alias("target")
        .merge(bronze_df_playlist_items.alias("source"),"target.video_id = source.video_id")
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

# ## Step 3: Raw Video Statistics JSON data to table

# CELL ********************

channel_path =  "Files/youtube_data_v3/videos/"

most_recent_file = get_most_recent_file(channel_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_videostats_json_to_df(most_recent_file): 

    # Read the JSON file
    df = spark.read.option("multiLine", "true").json(most_recent_file.path)

    # Select and flatten to your desired structure
    df_final = df.select(
        col("id").alias("video_id"),
        col("statistics.viewCount").cast("int").alias("video_view_count"),
        col("statistics.likeCount").cast("int").alias("video_like_count"),
        col("statistics.commentCount").cast("int").alias("video_comment_count"),
        current_timestamp().alias("loading_TS")
    )

    return df_final

bronze_df_videos = flatten_videostats_json_to_df(most_recent_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get the lakehouse abfs path
base_write_path = construct_abfs_path() 

# add on the full-path to the table
full_write_path = f"{base_write_path}Tables/youtube/videos"

delta_table = DeltaTable.forPath(spark, full_write_path)

# this time, because we have all the data in our df, we will use MERGE. 
( 
    delta_table.alias("target")
        .merge(bronze_df_videos.alias("source"),"target.video_id = source.video_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll() 
        .execute()
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
