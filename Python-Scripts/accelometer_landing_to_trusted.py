import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Amazon S3
AmazonS3_node1766556649463 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AmazonS3_node1766556649463")

# Script generated for node Amazon S3
AmazonS3_node1766556679449 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AmazonS3_node1766556679449")

# Script generated for node customer privacy filter
customerprivacyfilter_node1766556655260 = Join.apply(frame1=AmazonS3_node1766556649463, frame2=AmazonS3_node1766556679449, keys1=["user"], keys2=["email"], transformation_ctx="customerprivacyfilter_node1766556655260")

# Script generated for node Drop Fields
DropFields_node1766556921871 = DropFields.apply(frame=customerprivacyfilter_node1766556655260, paths=["customername", "phone", "email", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1766556921871")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1766556921871, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766556614100", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766556659151 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1766556921871, connection_type="s3", format="json", connection_options={"path": "s3://glu-s3link-s3/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1766556659151")

job.commit()