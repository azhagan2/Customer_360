# jobs/purchase_behavior_etl.py

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from glue_etl_pipeline.transformations.customer_ranking import transform_top_customers_sql
from glue_etl_pipeline.utils import get_glue_logger, write_to_s3

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_TARGET_PATH"])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = get_glue_logger()
s3_output_path = args["S3_TARGET_PATH"] + args["JOB_NAME"]

try:
    logger.info(f"Running ETL Job: {args['JOB_NAME']}")

    # Read from Glue catalog
    customer_df = spark.read.table("bronze_db.customers_raw")
    order_df = spark.read.table("bronze_db.orders_raw")
    
    customer_df.createOrReplaceTempView("customers")
    order_df.createOrReplaceTempView("orders")

    # Transform
    top_customers = transform_top_customers_sql(spark)

    # Write output
    write_to_s3(top_customers, s3_output_path)

    logger.info("ETL Job completed successfully")

except Exception as e:
    logger.error(f"ETL Job Failed: {e}")
    raise e

job.commit()
