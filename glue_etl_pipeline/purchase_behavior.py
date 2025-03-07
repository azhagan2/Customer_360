import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from glue_etl_pipeline.utils import get_glue_logger,read_from_rds,write_to_s3
from glue_etl_pipeline.glue_config import USER_MYSQL_URL,ORDER_MYSQL_URL,PRODUCT_MYSQL_URL


# Parse job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_TARGET_PATH"])

# Initialize Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
s3_output_path =args['S3_TARGET_PATH'] +args["JOB_NAME"]

# Initialize Logger
logger = get_glue_logger()

def run_etl():
    try:
        print("Staring ETL Job " +args["JOB_NAME"])
        customer_df =read_from_rds(spark,USER_MYSQL_URL,"Customers")
        order_df =read_from_rds(spark,ORDER_MYSQL_URL,"Orders")
        products_df =read_from_rds(spark,PRODUCT_MYSQL_URL,"Products")

        #common tranformation 
        top_customers=transform_sql()
        #top_customers=transform_dataframe(order_df,customer_df)

        write_to_s3(top_customers,s3_output_path)

        print("ETL Job Completed Successfully")
    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()



def transform_sql():
            # Run Spark SQL Query
        top_customers = spark.sql("""
            WITH customer_spending AS (
                SELECT
                    o.customer_id,
                    SUM(o.total_amount) AS total_spent,
                    COUNT(o.order_id) AS total_orders,
                    MAX(o.order_date) AS last_purchase_date
                FROM Orders o
                WHERE o.order_date >= date_add(current_date(), -365)  -- Last 1 year
                GROUP BY o.customer_id
            ),
            customer_ranking AS (
                SELECT
                    c.country,
                    c.customer_id,
                    c.first_name,
                    c.email,
                    cs.total_spent,
                    cs.total_orders,
                    cs.last_purchase_date,
                    RANK() OVER (PARTITION BY c.country ORDER BY cs.total_spent DESC) AS spending_rank
                FROM customer_spending cs
                JOIN Customers c ON cs.customer_id = c.customer_id
            )
            SELECT * FROM customer_ranking WHERE spending_rank <= 10 and country like 'United %';
        """)
        return top_customers


def transform_dataframe(order_df,customer_df):
    # Filter last 1 year of data
    one_year_ago = F.date_add(F.current_date(), -365)
    filtered_orders = order_df.filter(F.col("order_date") >= one_year_ago)

    # Aggregate customer spending
    customer_spending = (
        filtered_orders.groupBy("customer_id")
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.count("order_id").alias("total_orders"),
            F.max("order_date").alias("last_purchase_date")
        )
    )

    # Join with customers table
    customer_data = customer_spending.join(customer_df, "customer_id")

    # Define window specification for ranking
    window_spec = Window.partitionBy("country").orderBy(F.desc("total_spent"))

    # Add ranking column
    customer_ranking = customer_data.withColumn("spending_rank", F.rank().over(window_spec))

    # Filter top 100 customers
    top_customers = customer_ranking.filter( (F.col("spending_rank") <= 10) &  (F.col("country").like("United %")))

    # Show results
    top_customers.select("country","customer_id","first_name","email","total_spent","total_orders","spending_rank").show(20)

    return top_customers