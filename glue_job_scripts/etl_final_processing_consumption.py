import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, ArrayType, TimestampType, StructType, StructField


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_path', 'input_path'])


# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Upsert Data Aggregations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()
    
    
output_path = args['output_path']
input_path = args['input_path']


# input_path = "s3://youtube-analytics-data/etl_data/curated/'


vid_df = spark.read.format('delta').load(f'{input_path}curated_video_data/')
vid_df = vid_df.filter(F.col('updated_date') == F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC'))).select('*')

# vid_df.show(5)

print("read the curated video dataframe")

vid_delta_table_exists = DeltaTable.isDeltaTable(spark, f"{output_path}video_data_dim/")

if vid_delta_table_exists:
    
    print("Video Target data is present and about to read as DeltaTable")

    vid_delta_table = DeltaTable.forPath(spark, f"{output_path}video_data_dim/")
    
    vid_delta_table.alias('tgt') \
      .merge(
        vid_df.alias('src'),
        'tgt.video_id = src.video_id'
      ) \
      .whenMatchedUpdate(set =
        {
            "title": "src.title",
            "description": "src.description",
            "hashtags": "src.hashtags",
            "audio_language": "src.audio_language",
            "content_duration_hour": "src.content_duration_hour",
            "content_duration_minutes": "src.content_duration_minutes",
            "content_duration_seconds": "src.content_duration_seconds",
            "privacy_status": "src.privacy_status",
            "topic_category": "src.topic_category",
            "updated_date": "src.updated_date",
        }
      ) \
      .whenNotMatchedInsert(values =
        {
            "video_id": "src.video_id",
            "channel_id": "src.channel_id",
            "published_date": "src.published_date",
            "virtual_dimension": "src.virtual_dimension",
            "quality_definition": "src.quality_definition",
            "title": "src.title",
            "description": "src.description",
            "hashtags": "src.hashtags",
            "audio_language": "src.audio_language",
            "content_duration_hour": "src.content_duration_hour",
            "content_duration_minutes": "src.content_duration_minutes",
            "content_duration_seconds": "src.content_duration_seconds",
            "privacy_status": "src.privacy_status",
            "topic_category": "src.topic_category",
            "updated_date": "src.updated_date",
        }
      ) \
      .execute()
else:
    print("Video Target data is not present and about to write as Delta format")
    vid_df.select("video_id", "channel_id", "published_date", "virtual_dimension", "quality_definition", "title", "description", "audio_language", "content_duration_hour", "content_duration_minutes", "content_duration_seconds", "hashtags", "privacy_status", "topic_category", 'updated_date')\
    .write.format("delta").partitionBy('channel_id').mode("overwrite").save(f"{output_path}video_data_dim/")
    


chnl_df = spark.read.format('delta').load(f'{input_path}curated_channel_data/')
chnl_df = chnl_df.filter(F.col('updated_date') == F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC'))).select('*')

# chnl_df.show(5)

chnl_delta_table_exists = DeltaTable.isDeltaTable(spark, f"{output_path}channel_data_dim/")

if chnl_delta_table_exists:
    print("Channel Target data is present and about to read as DeltaTable")
    
    chnl_delta_table = DeltaTable.forPath(spark, f"{output_path}channel_data_dim/")
    
    chnl_delta_table.alias('tgt') \
      .merge(
        chnl_df.alias('src'),
        'tgt.channel_id = src.channel_id'
      ) \
      .whenMatchedUpdate(set =
        {
            "title": "src.title",
            "description": "src.description",
            "default_language": "src.default_language",
            "country": "src.country",
            "updated_date": "src.updated_date",
        }
      ) \
      .whenNotMatchedInsert(values =
        {
            "channel_id": "src.channel_id",
            "title": "src.title",
            "description": "src.description",
            "published_date": "src.published_date",
            "default_language": "src.default_language",
            "country": "src.country",
            "updated_date": "src.updated_date",
        }
      ) \
      .execute()
else:
    print("Channel Target data is not present and about to write as Delta format")
    chnl_df.select("channel_id", "title", "description", "published_date", "default_language", "country", "updated_date").write.format("delta").mode("overwrite").save(f"{output_path}channel_data_dim/")


print("Reading date dim table from curated location")
date_dim_df = spark.read.format('parquet').load(f"{input_path}date_dim/")
date_dim_df.show()

curr_date_key = date_dim_df.filter(F.col('date') == F.to_utc_timestamp(F.current_date(), 'UTC')).select('datekey').first()[0]

channel_data_fact_df = chnl_df.select('channel_id', 'views_count','subscribers_count','videos_count', F.lit(curr_date_key).alias('snapshot_date'))
channel_data_fact_df.show()

print("Writing the channel fact table as delta")
channel_data_fact_df.write.format('delta').partitionBy('snapshot_date').save(f"{output_path}channel_data_fact/")


video_data_fact_df = vid_df.select('video_id', 'channel_id', 'views_count', 'likes_count', 'dislikes_count', 'favorites_count', 'comments_count', F.lit(curr_date_key).alias('snapshot_date'))
video_data_fact_df.show(10)

print("Writing the video fact table as delta")
video_data_fact_df.write.format('delta').partitionBy('snapshot_date').save(f"{output_path}video_data_fact/")

spark.stop()