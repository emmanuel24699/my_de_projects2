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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749959452950 = glueContext.create_dynamic_frame.from_catalog(database="rental_db", table_name="rental_db_apartments", transformation_ctx="AWSGlueDataCatalog_node1749959452950",
    additional_options={
        "bookmarkKeys": ["id"]
    }
    )

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749960484111 = glueContext.create_dynamic_frame.from_catalog(database="rental_db", table_name="rental_db_bookings", transformation_ctx="AWSGlueDataCatalog_node1749960484111",
    additional_options={
        "bookmarkKeys": ["booking_id"]
    }
    )

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749960685219 = glueContext.create_dynamic_frame.from_catalog(database="rental_db", table_name="rental_db_apartment_attributes", transformation_ctx="AWSGlueDataCatalog_node1749960685219",
    additional_options={
        "bookmarkKeys": ["id"]
    }
    )

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1749960755315 = glueContext.create_dynamic_frame.from_catalog(database="rental_db", table_name="rental_db_user_viewing", transformation_ctx="AWSGlueDataCatalog_node1749960755315",
    additional_options={
        "bookmarkKeys": ["user_id", "apartment_id", "viewed_at"]
    })

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AWSGlueDataCatalog_node1749959452950, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749959394353", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749959889034 = glueContext.write_dynamic_frame.from_options(frame=AWSGlueDataCatalog_node1749959452950, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-analytics/data/raw/apartments/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1749959889034")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AWSGlueDataCatalog_node1749960484111, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749959394353", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749960523729 = glueContext.write_dynamic_frame.from_options(frame=AWSGlueDataCatalog_node1749960484111, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-analytics/data/raw/bookings/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1749960523729")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AWSGlueDataCatalog_node1749960685219, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749959394353", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749960709786 = glueContext.write_dynamic_frame.from_options(frame=AWSGlueDataCatalog_node1749960685219, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-analytics/data/raw/apartment_attributes/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1749960709786")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AWSGlueDataCatalog_node1749960755315, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1749959394353", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1749960789191 = glueContext.write_dynamic_frame.from_options(frame=AWSGlueDataCatalog_node1749960755315, connection_type="s3", format="glueparquet", connection_options={"path": "s3://lab2-rental-analytics/data/raw/user_viewing/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1749960789191")

job.commit()