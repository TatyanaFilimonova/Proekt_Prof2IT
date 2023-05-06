import unittest

from pyspark.sql import SparkSession

from get_actor_roles_by_title import get_actor_roles_by_title

class TestGetActoresRolesByTitle(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Unit-tests")
                      .getOrCreate())


    def tearDown(self):
        self.spark.stop()


    def test_get_actor_roles_by_title(self):
        spark = SparkSession.builder.getOrCreate()

        # Create test DataFrames
        name_data = [("1", "John Smith"), ("2", "Alice Johnson"), ("3", "Bob Williams")]
        name_df = spark.createDataFrame(name_data, ["nconst", "primaryName"])

        title_principals_data = [("1", "actor", "1", "Dad"), ("2", "director", "2", ""), ("3", "actress", "3", "Son")]
        title_principals = spark.createDataFrame(title_principals_data, ["nconst", "category", "tconst", "characters"])

        title_akas_data = [("1", "Movie 1"), ("2", "Movie 2"), ("3", "Movie 3")]
        title_akas = spark.createDataFrame(title_akas_data, ["tconst", "title"])

        output_file = "/path/to/output.csv"

        # Call the function
        result = get_actor_roles_by_title(name_df, title_principals, title_akas, output_file)
        result.show()

        # Verify the output
        assert result.count() == 2  # Check the number of rows in the result DataFrame

        # Clean up the output file

