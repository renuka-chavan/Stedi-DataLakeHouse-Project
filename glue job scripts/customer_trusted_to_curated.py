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
AccelerometerTrusted_node1769060372245 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-database", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1769060372245")

# Script generated for node Customer Trusted
CustomerTrusted_node1769060353979 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-database", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1769060353979")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct c.*
from customer_trusted c
inner join accelerometer_trusted a
on c.email = a.user;
'''
SQLQuery_node1769060357132 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1769060353979, "accelerometer_trusted":AccelerometerTrusted_node1769060372245}, transformation_ctx = "SQLQuery_node1769060357132")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769060357132, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769060314297", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769060361701 = glueContext.getSink(path="s3://stedi-datalakehouse-bucket/customer/curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769060361701")
AmazonS3_node1769060361701.setCatalogInfo(catalogDatabase="stedi-datalakehouse-database",catalogTableName="customer_curated")
AmazonS3_node1769060361701.setFormat("json")
AmazonS3_node1769060361701.writeFrame(SQLQuery_node1769060357132)
job.commit()