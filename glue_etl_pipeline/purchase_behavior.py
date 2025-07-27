import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from transformations.customer_ranking import transform_top_customers_sql, transform_dataframe    
from glue_etl_pipeline.utils import get_glue_logger,write_to_s3

# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH', 'INPUT_DB'])


# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3_output_path =args['S3_TARGET_PATH'] +args["JOB_NAME"]
bronze_db = args['INPUT_DB']
from glue_etl_pipeline.utils import get_glue_logger,write_to_s3,write_audit_log,update_control_table
from datetime import datetime

# Initialize Logger
logger = get_glue_logger()

def run_etl():
    try:
        start_time = datetime.now()
        print("Staring ETL Job " +args["JOB_NAME"])

        print("starting Puchase Behaviour    --->  ")
        print("S3 Target Path: " + s3_output_path)
        print("  starting transformation")


        customer_df = spark.read.table(f"{bronze_db}.customers")
        order_df = spark.read.table(f"{bronze_db}.orders")
        customer_df.createOrReplaceTempView("customers")
        order_df.createOrReplaceTempView("orders")

        customer_df.show()
        
        #common tranformation 
        top_customers=transform_top_customers_sql(spark)
        print("Running  SQL Query  for top customers    --->")


        top_customers.show()
        print(top_customers.count())
        #top_customers=transform_dataframe(order_df,customer_df)

        
        write_to_s3(top_customers,s3_output_path)

        print("ETL Job Completed Successfully")
        end_time = datetime.now()
        write_audit_log(spark, args['JOB_NAME'], "SUCCESS", top_customers, start_time, end_time)
        update_control_table(spark, args['JOB_NAME'], "SUCCESS")

    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        write_audit_log(spark, args['JOB_NAME'], "FAILURE", 0, start_time, end_time)
        update_control_table(spark, args['JOB_NAME'], "FAILURE")
        raise e
    job.commit()

