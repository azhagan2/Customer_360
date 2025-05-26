import unittest
from pyspark.sql import SparkSession
from glue_job import transform_data


class TestGlueJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("GlueTest") \
            .master("local[1]") \
            .getOrCreate()

    def test_transform_data(self):
        data = [("Alice", "active"), ("Bob", "inactive"), ("Charlie", "active")]
        columns = ["name", "status"]
        df = self.spark.createDataFrame(data, columns)

        result_df = transform_data(df)

        expected_data = [("Alice",), ("Charlie",)]
        expected_df = self.spark.createDataFrame(expected_data, ["user_name"])

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
