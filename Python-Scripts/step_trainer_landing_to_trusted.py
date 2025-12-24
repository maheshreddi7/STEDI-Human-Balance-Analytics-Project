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

# Script generated for node custmer_curated
custmer_curated_node1766563175716 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://glu-s3link-s3/customer/curated/"], "recurse": True}, transformation_ctx="custmer_curated_node1766563175716")

# Script generated for node step_landing
step_landing_node1766563174496 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://glu-s3link-s3/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_landing_node1766563174496")

# Script generated for node SQL Query
SqlQuery1 = '''
select s.serialnumber,s.sensorreadingtime,s.distancefromobject from s join c 
on s.serialnumber  = c.serialnumber
'''
SQLQuery_node1766563688584 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"s":step_landing_node1766563174496, "c":custmer_curated_node1766563175716}, transformation_ctx = "SQLQuery_node1766563688584")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct serialNumber,sensorreadingtime,distancefromobject from myDataSource
'''
SQLQuery_node1766563179575 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":SQLQuery_node1766563688584}, transformation_ctx = "SQLQuery_node1766563179575")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1766563179575, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766563169408", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766563182577 = glueContext.getSink(path="s3://glu-s3link-s3/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766563182577")
AmazonS3_node1766563182577.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_landing_trusted")
AmazonS3_node1766563182577.setFormat("json")
AmazonS3_node1766563182577.writeFrame(SQLQuery_node1766563179575)
job.commit()