import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1769062476330 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-database", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1769062476330")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1769062467432 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-database", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1769062467432")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.*, s.distanceFromObject
from step_trainer_trusted s
inner join accelerometer_trusted a
on s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1769062470607 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1769062467432, "accelerometer_trusted":AccelerometerTrusted_node1769062476330}, transformation_ctx = "SQLQuery_node1769062470607")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769062470607, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769062428709", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769062473092 = glueContext.getSink(path="s3://stedi-datalakehouse-bucket/step_trainer/curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769062473092")
AmazonS3_node1769062473092.setCatalogInfo(catalogDatabase="stedi-datalakehouse-database",catalogTableName="machine_learning_curated")
AmazonS3_node1769062473092.setFormat("json")
AmazonS3_node1769062473092.writeFrame(SQLQuery_node1769062470607)
job.commit()