import unittest

from pyspark.sql import SparkSession

from get_actor_roles_by_title import get_actor_roles_by_title
from get_top_adult_movies_region import get_top_adult_movies_region


class TestGetActoresRolesByTitle(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Unit-tests")
                      .getOrCreate())


    def tearDown(self):
        self.spark.stop()


    def test_getadults_movies_regions(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import sum, when, col, count

        # Create test DataFrames
        title_akas_data = [("1", "Movie 1", "US", 1), ("2", "Movie 2", "US", 1), ("3", "Movie 3", "UK", 1)]
        title_akas = self.spark.createDataFrame(title_akas_data, ["titleId", "title", "region", "isAdult"])

        title_basics_data = [("1", "Movie 1"), ("2", "Movie 2"), ("3", "Movie 3")]
        title_basics = self.spark.createDataFrame(title_basics_data, ["tconst", "title"])

        # Call the function
        result = get_top_adult_movies_region(title_akas, title_basics)

        # Verify the output
        assert result.count() == 2  # Check the number of rows in the result DataFrame

