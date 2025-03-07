import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql.functions import sum, count, col
from pyspark.sql.functions import col, avg, sum, date_format, year, month
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
        customer_df =read_from_rds(spark,USER_MYSQL_URL,"Customers")
        order_df =read_from_rds(spark,ORDER_MYSQL_URL,"Orders")
        order_items_df =read_from_rds(spark,ORDER_MYSQL_URL,"Order_Items")
        products_df =read_from_rds(spark,PRODUCT_MYSQL_URL,"Products")

        #common tranformation 
        #(monthly_trends,quarterly_trends)=transform_sql()
        (monthly_trends,quarterly_trends)=transform_dataframe(order_df,products_df,order_items_df)

        write_to_s3(monthly_trends,s3_output_path)
        write_to_s3(quarterly_trends,s3_output_path)

        print("ETL Job Completed Successfully")
    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    job.commit()



def transform_sql():
    # Run Spark SQL Query
    spark.sql("""
                WITH sales_data AS (
                    SELECT 
                        o.order_id,
                        o.customer_id,
                        o.order_date,
                        oi.product_id,
                        oi.quantity,
                        oi.unit_price,
                        (oi.quantity * oi.unit_price) AS sales_amount
                    FROM Orders o
                    INNER JOIN Order_Items oi ON o.order_id = oi.order_id
                    INNER JOIN Products p ON oi.product_id = p.product_id
                ),

                sales_data_with_time AS (
                    SELECT 
                        s.*,
                        YEAR(s.order_date) AS order_year,
                        MONTH(s.order_date) AS order_month,
                        QUARTER(s.order_date) AS order_quarter
                    FROM sales_data s
                ),

                monthly_trends AS (
                    SELECT 
                        product_id,
                        order_year,
                        order_month,
                        AVG(unit_price) AS avg_unit_price,
                        SUM(sales_amount) AS total_sales,
                        SUM(quantity) AS total_qt
                    FROM sales_data_with_time
                    GROUP BY product_id, order_year, order_month
                ),

                quarterly_trends AS (
                    SELECT 
                        product_id,
                        order_year,
                        order_quarter,
                        AVG(unit_price) AS avg_unit_price,
                        SUM(sales_amount) AS total_sales,
                        SUM(quantity) AS total_qt
                    FROM sales_data_with_time
                    GROUP BY product_id, order_year, order_quarter
                )""").show()
     
    monthly_trends = spark.sql(""" SELECT * FROM monthly_trends ORDER BY product_id, order_year, order_month; """)
    quarterly_trends = spark.sql(""" SELECT * FROM quarterly_trends ORDER BY product_id, order_year, order_month; """)

    return (monthly_trends,quarterly_trends)


def transform_dataframe(order_df,products_df,order_items_df):



    # Join orders with order_items to get product-level sales
    sales_data = order_df.join(order_items_df, "order_id", "inner") \
        .join(products_df, "product_id", "inner") \
        .select(
            order_df.order_id,
            order_df.customer_id,
            order_df.order_date,
            order_items_df.product_id,
            order_items_df.quantity,
            order_items_df.unit_price,
            (order_items_df.quantity * order_items_df.unit_price).alias("sales_amount")
        )

    historical_customer_sales_df=spark.read.parquet("s3://feb2025-training-bucket/RetailCustomer360/historical_sales_data")
    #sales_data = sales_data.withColumn("order_year", year(col("order_date"))) .withColumn("order_month", month(col("order_date"))) 
    #historical_customer_sales_df.count()
    #sales_data = sales_data.union(historical_customer_sales_df)
    #historical_customer_sales_df.printSchema()
    #sales_data.printSchema()

    sales_data = sales_data.withColumn("order_year", year(col("order_date"))) \
        .withColumn("order_month", month(col("order_date"))) 



    # Extract Year, Month, and Quarter
    df = sales_data.withColumn("order_quarter", date_format(col("order_date"), "Q"))

    # Calculate Seasonal Trends
    # 1. Monthly Price Trend
    monthly_trends = df.groupBy("product_id","order_year", "order_month").agg(
        avg("unit_price").alias("avg_unit_price"),
        sum("sales_amount").alias("total_sales"),
        sum("quantity").alias("total_qt"),
    ).orderBy("product_id","order_year", "order_month")

    # 2. Quarterly Price Trend
    quarterly_trends = df.groupBy("product_id","order_year", "order_quarter").agg(
        avg("unit_price").alias("avg_unit_price"),
        sum("sales_amount").alias("total_sales"),
        sum("quantity").alias("total_qt"),
    ).orderBy("product_id","order_year", "order_quarter")

    # Show Results
    monthly_trends.show()
    quarterly_trends.show()


    return (monthly_trends,quarterly_trends)