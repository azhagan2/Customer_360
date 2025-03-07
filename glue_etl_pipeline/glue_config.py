import os

# S3 Paths
S3_SOURCE_PATH = os.getenv("S3_SOURCE_PATH", "s3://your-bucket-name/raw-data/")
S3_TARGET_PATH = "s3://feb2025-training-bucket/analytics"

# Glue Catalog Database & Table
GLUE_DATABASE = "customer_analytics"
GLUE_TABLE = "customer_transactions"


USER_MYSQL_URL = "jdbc:mysql://rds-mysql-instance.cjq8msca2hav.ap-south-1.rds.amazonaws.com:3306/UserService"
ORDER_MYSQL_URL = "jdbc:mysql://rds-mysql-instance.cjq8msca2hav.ap-south-1.rds.amazonaws.com:3306/OrderService"
PRODUCT_MYSQL_URL = "jdbc:mysql://rds-mysql-instance.cjq8msca2hav.ap-south-1.rds.amazonaws.com:3306/ProductService"
MYSQL_PROPERTIES = {
    'user': 'admin',
    'password': 'vmsKuOY~nk38|^l#~',
    'driver': 'com.mysql.cj.jdbc.Driver',
    'allowPublicKeyRetrieval':'true',
    'useSSL':'false'
}