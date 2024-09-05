import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, ArrayType, TimestampType, StructType, StructField


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_path', 'input_path'])



# Initialize the Spark session with delta lake compatible configurations
spark = SparkSession.builder \
    .appName("Delta Lake Upsert Data Aggregations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()
    

# collect the essential job params
output_dest_loc = args['output_path']
input_source_loc = args['input_path']





def channel_data_cleaning():

    # schema for the channel raw data
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
    
    # selecting required cols
    channel_df = channel_df.select("channel_id", "title", "description", F.to_date(F.col('published_at')).alias('published_date'), "default_language", "country", "views_count", "subscribers_count", "videos_count", F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC')).alias('updated_date'))
    
    # writing the data as delta format to S3
    channel_df.write.format('delta').partitionBy('updated_date').mode('append').save(f'{output_dest_loc}curated_channel_data/')
    

def video_data_cleaning():

    # schema for video raw data

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
    
    # list of channel ids which I can retrive from glue job params as well from the orchestartion.
    channel_lst = ["UC0GP1HDhGZTLih7B89z_cTg", "UC4p_I9eiRewn2KoU-nawrDg", "UCrbQxu0YkoVWu2dw5b1MzNg", "UCs0xZ60FSNxFxHPVFFsXNTA", "UC_WgSFSkn7112rmJQcHSUIQ"]
    
    # checking out the clean and corrupted records for the data insights
    clean_filter_cond = ~(~F.col('channel_id').isin(channel_lst) | F.col('channel_id').isNull())
    corrupt_filter_cond = (~F.col('channel_id').isin(channel_lst) | F.col('channel_id').isNull())
    
    vid_cleaned_df = vid_df.filter(clean_filter_cond)
    
    # finding the null counts in all the cols
    vid_cleaned_stats_df = vid_cleaned_df.select([F.count(F.when(F.col(colu).isNull() | F.col(colu).contains('NULL'), colu)).alias(colu) for colu in vid_df.columns])
    vid_cleaned_stats_df.show(truncate=False)
    
    # finding the corrupted records count
    vid_corrupted_df = vid_df.filter(corrupt_filter_cond)
    print("Count of Corrupted records are", vid_corrupted_df.count())

    vid_df = vid_cleaned_df.select('*')
    
    # sample_video_id = "3VNUe0rqBi0"
    # vid_df = vid_df.filter(F.length(F.col('video_id')) == F.lit(len(sample_id)).alias('length_val')).select(F.col('video_id'), 'channel_id').show(50)
    
    # selecting the required cols with come modification on the data format
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
         'topic_category',
         'views_count',
         'likes_count',
         'dislikes_count',
         'favorites_count',
         'comments_count',
          F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC')).alias('updated_date')
    )
    
    # writing the data as delta format back to S3 location
    video_df.write.format('delta').partitionBy('updated_date').mode('append').save(f'{output_dest_loc}curated_video_data/')

try:
    channel_data_cleaning()
    video_data_cleaning()
except Exception as e:
    print("Error occurred -", e)
    raise Exception(e)

spark.stop()