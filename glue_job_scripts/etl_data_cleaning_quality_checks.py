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
    
    
output_dest_loc = args['output_path']
input_source_loc = args['input_path']

def channel_data_cleaning():
    channel_schema = StructType([
        StructField('channel_id', StringType(), True),
        StructField('title', StringType(), True),
        StructField('description', StringType(), True),
        StructField('published_at', TimestampType(), True),
        StructField('default_language', StringType(), True),
        StructField('country', StringType(), True),
        StructField('views_count', IntegerType(), True),
        StructField('subscribers_count', IntegerType(), True),
        StructField('videos_count', IntegerType(), True),
    ])
    
    channel_df = spark.read.format('csv').option('header', True).option('multiline', True).schema(channel_schema).load(f"{input_source_loc}channel/youtube_channels_data.csv")
    channel_df.show()
    
    channel_df = channel_df.select("channel_id", "title", "description", F.to_date(F.col('published_at')).alias('published_date'), "default_language", "country", "views_count", "subscribers_count", "videos_count", F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC')).alias('updated_date'))
    
    channel_df.write.format('delta').partitionBy('updated_date').option('mode', 'append').save(f'{output_dest_loc}curated_channel_data')
    

def video_data_cleaning():
    vid_schema = StructType([
        StructField('video_id', StringType(), True),
        StructField('channel_id', StringType(), True),
        StructField('published_at', StringType(), True),
        StructField('title', StringType(), True),
        StructField('description', StringType(), True),
        StructField('hashtags', StringType(), True),
        StructField('audio_language', StringType(), True),
        StructField('content_duration', StringType(), True),
        StructField('virtual_dimension', StringType(), True),
        StructField('quality_definition', StringType(), True),
        StructField('privacy_status', StringType(), True),
        StructField('topic_category', StringType(), True),
        StructField('views_count', IntegerType(), True),
        StructField('likes_count', IntegerType(), True),
        StructField('dislikes_count', IntegerType(), True),
        StructField('favorites_count', IntegerType(), True),
        StructField('comments_count', IntegerType(), True),
    ])
    
    vid_df = spark.read.format('csv').option('header', True).option('multiline', True).schema(vid_schema).load(f"{input_source_loc}video/youtube_videos_data.csv")
    vid_df.show()
    
    vid_df.printSchema()
    
    
    video_df = vid_df.select('video_id',
        'channel_id',
         F.to_date(F.col('published_at')).alias('published_date'),
         'title',
         'description',
         F.split(F.col('hashtags'), ',').alias('hashtags'),
         'audio_language',
         F.when(F.regexp_extract(F.col('content_duration'), r'PT((\d+)H)?((\d+)M)?((\d+)S)?', 2) > 0, F.regexp_extract(F.col('content_duration'), r'PT((\d+)H)?((\d+)M)?((\d+)S)?', 2)).otherwise(0).alias('content_duration_hour'),
         F.when(F.regexp_extract(F.col('content_duration'), r'PT((\d+)H)?((\d+)M)?((\d+)S)?', 4) > 0, F.regexp_extract(F.col('content_duration'), r'PT((\d+)H)?((\d+)M)?((\d+)S)?', 4)).otherwise(0).alias('content_duration_minutes'),
         F.when(F.regexp_extract(F.col('content_duration'), r'PT((\d+)H)?((\d+)M)?((\d+)S)?', 6) > 0, F.regexp_extract(F.col('content_duration'), r'PT((\d+)H)?((\d+)M)?((\d+)S)?', 6)).otherwise(0).alias('content_duration_seconds'),
         'virtual_dimension',
         'quality_definition',
         'privacy_status',
         F.split(F.col('topic_category'), ',').alias('topic_category'),
         'views_count',
         'likes_count',
         'dislikes_count',
         'favorites_count',
         'comments_count',
          F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC')).alias('updated_date')
    )
    
    video_df.write.format('delta').partitionBy('updated_date').option('mode', 'append').save(f'{output_dest_loc}curated_video_data')


channel_data_cleaning()
video_data_cleaning()

spark.stop()