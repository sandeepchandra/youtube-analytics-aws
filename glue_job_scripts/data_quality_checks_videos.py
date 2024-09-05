import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node Filter Latest Date
def MyTransform_1(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F
    df = dfc.select(list(dfc.keys())[0]).toDF()
    vid_filtered_df = df.filter(F.col('updated_date') == F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC'))).select('*')
    dyf_filtered_df = DynamicFrame.fromDF(vid_filtered_df, glueContext, "filtered_latest_date")
    return(DynamicFrameCollection({"CustomTransform0": dyf_filtered_df}, glueContext))
# Script generated for node Adding Extra Cols
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F
    df = dfc.select(list(dfc.keys())[0]).toDF()
    curr_date = F.to_date(F.to_utc_timestamp(F.current_date(), 'UTC'))
    new_cols_df = df.select('*', curr_date.alias('updated_at'), F.regexp_replace(curr_date, '-', '').alias('datekey'))
    dyf_new_cols = DynamicFrame.fromDF(new_cols_df, glueContext, "new_cols_added")
    return(DynamicFrameCollection({"CustomTransform0": dyf_new_cols}, glueContext))
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
additionalOptions={}
AmazonS3_node1725509619711_df = spark.read.format("delta").options(**additionalOptions).load("s3://youtube-analytics-data/etl_data/curated/curated_video_data/")
AmazonS3_node1725509619711 = DynamicFrame.fromDF(AmazonS3_node1725509619711_df, glueContext, "AmazonS3_node1725509619711")

# Script generated for node Filter Latest Date
FilterLatestDate_node1725513309310 = MyTransform_1(glueContext, DynamicFrameCollection({"AmazonS3_node1725509619711": AmazonS3_node1725509619711}, glueContext))

# Script generated for node Filter Selected Columns
FilterSelectedColumns_node1725513928607 = SelectFromCollection.apply(dfc=FilterLatestDate_node1725513309310, key=list(FilterLatestDate_node1725513309310.keys())[0], transformation_ctx="FilterSelectedColumns_node1725513928607")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1725509704977_ruleset = """

    Rules = [
        ColumnCount = 20,
        Completeness "channel_id" = 1,
        Completeness "video_id" = 1,
        DistinctValuesCount "channel_id" = 5,
        IsComplete "published_date",
        IsPrimaryKey "video_id"
    ]
"""

EvaluateDataQuality_node1725509704977 = EvaluateDataQuality().process_rows(frame=FilterSelectedColumns_node1725513928607, ruleset=EvaluateDataQuality_node1725509704977_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1725509704977", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1725510265476 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1725509704977, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1725510265476")

# Script generated for node Adding Extra Cols
AddingExtraCols_node1725510807598 = MyTransform(glueContext, DynamicFrameCollection({"ruleOutcomes_node1725510265476": ruleOutcomes_node1725510265476}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1725511978627 = SelectFromCollection.apply(dfc=AddingExtraCols_node1725510807598, key=list(AddingExtraCols_node1725510807598.keys())[0], transformation_ctx="SelectFromCollection_node1725511978627")

# Script generated for node Amazon S3
AmazonS3_node1725510482040 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1725511978627, connection_type="s3", format="glueparquet", connection_options={"path": "s3://youtube-analytics-data/data_quality_results/videos_data/", "partitionKeys": ["updated_at"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1725510482040")

job.commit()