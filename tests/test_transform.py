import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from glue_etl_pipeline.purchase_behavior import transform_sql

class TestTransformSQL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestTransformSQL") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Prepare test data for customers
        customers_data = [
            (1, "John", "Doe", "john@example.com", "United States"),
            (2, "Jane", "Smith", "jane@example.com", "United Kingdom"),
        ]
        customer_columns = ["customer_id", "first_name", "last_name", "email", "country"]
        self.spark.createDataFrame(customers_data, customer_columns) \
            .createOrReplaceTempView("customers")

        # Prepare test data for orders (only 1 year ago or newer)
        orders_data = [
            (1, 100.0, "2024-10-01", 101),
            (1, 120.0, "2024-11-01", 102),
            (2, 300.0, "2024-09-01", 201),
        ]
        order_columns = ["customer_id", "total_amount", "order_date", "order_id"]
        df = self.spark.createDataFrame(orders_data, order_columns)
        df = df.withColumn("order_date", F.to_date("order_date"))
        df.createOrReplaceTempView("orders")

    def test_transform_sql_output(self):
        result_df = transform_sql()
        result = result_df.collect()

        # Check that top customers are returned with correct columns
        self.assertGreater(len(result), 0)
        expected_cols = {
            "customer_id", "country", "full_name",
            "email", "total_spent", "total_orders",
            "last_purchase_date", "spending_rank"
        }
        self.assertTrue(expected_cols.issubset(set(result_df.columns)))

        # Optional: Check ranking or values
        us_customers = result_df.filter("country = 'United States'").collect()
        self.assertEqual(us_customers[0].customer_id, 1)


if __name__ == '__main__':
    unittest.main()
