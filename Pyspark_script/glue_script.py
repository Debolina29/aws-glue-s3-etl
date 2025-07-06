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
AmazonS3_node1751806258614 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://glue-etl-project-debolina/employees.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1751806258614")

# Script generated for node Rename Field
RenameField_node1751806395684 = RenameField.apply(frame=AmazonS3_node1751806258614, old_name="salary", new_name="monthly_salary", transformation_ctx="RenameField_node1751806395684")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=RenameField_node1751806395684, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751806218061", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1751806439282 = glueContext.write_dynamic_frame.from_options(frame=RenameField_node1751806395684, connection_type="s3", format="csv", connection_options={"path": "s3://glue-etl-project-debolina/output/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1751806439282")

job.commit()
