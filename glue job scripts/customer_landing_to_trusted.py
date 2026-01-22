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

# Script generated for node Customer Landing
CustomerLanding_node1769059031519 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-database", table_name="customer_landing", transformation_ctx="CustomerLanding_node1769059031519")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from customer_landing
where sharewithresearchasofdate is not null;

'''
SQLQuery_node1769059035862 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":CustomerLanding_node1769059031519}, transformation_ctx = "SQLQuery_node1769059035862")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769059035862, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769058903036", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1769059039431 = glueContext.getSink(path="s3://stedi-datalakehouse-bucket/customer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1769059039431")
AmazonS3_node1769059039431.setCatalogInfo(catalogDatabase="stedi-datalakehouse-database",catalogTableName="customer_trusted")
AmazonS3_node1769059039431.setFormat("json")
AmazonS3_node1769059039431.writeFrame(SQLQuery_node1769059035862)
job.commit()