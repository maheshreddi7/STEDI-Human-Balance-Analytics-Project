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

# Script generated for node accelerometer_landing
accelerometer_landing_node1766556649463 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://glu-s3link-s3/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1766556649463")

# Script generated for node customer_trusted
customer_trusted_node1766556679449 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://glu-s3link-s3/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1766556679449")

# Script generated for node customer privacy filter
customerprivacyfilter_node1766556655260 = Join.apply(frame1=accelerometer_landing_node1766556649463, frame2=customer_trusted_node1766556679449, keys1=["user"], keys2=["email"], transformation_ctx="customerprivacyfilter_node1766556655260")

# Script generated for node Drop Fields
DropFields_node1766556921871 = DropFields.apply(frame=customerprivacyfilter_node1766556655260, paths=["customername", "phone", "email", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1766556921871")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1766556921871, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766556614100", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766556659151 = glueContext.getSink(path="s3://glu-s3link-s3/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766556659151")
AmazonS3_node1766556659151.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_landing_to_trusted")
AmazonS3_node1766556659151.setFormat("json")
AmazonS3_node1766556659151.writeFrame(DropFields_node1766556921871)
job.commit()