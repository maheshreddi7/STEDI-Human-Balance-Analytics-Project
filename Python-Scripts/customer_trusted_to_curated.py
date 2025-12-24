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

# Script generated for node accelerometer_landing
accelerometer_landing_node1766556649463 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1766556649463")

# Script generated for node customer_trusted
customer_trusted_node1766556679449 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1766556679449")

# Script generated for node joins
SqlQuery0 = '''
select *  from a join c on a.user = c.email

'''
joins_node1766559234751 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":accelerometer_landing_node1766556649463, "c":customer_trusted_node1766556679449}, transformation_ctx = "joins_node1766559234751")

# Script generated for node Drop fields & drop duplicates
SqlQuery1 = '''
SELECT DISTINCT
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
FROM myDataSource;

'''
Dropfieldsdropduplicates_node1766559624229 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":joins_node1766559234751}, transformation_ctx = "Dropfieldsdropduplicates_node1766559624229")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=Dropfieldsdropduplicates_node1766559624229, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766556614100", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1766556659151 = glueContext.getSink(path="s3://glu-s3link-s3/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1766556659151")
customer_curated_node1766556659151.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1766556659151.setFormat("json")
customer_curated_node1766556659151.writeFrame(Dropfieldsdropduplicates_node1766559624229)
job.commit()