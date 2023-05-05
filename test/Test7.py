# Тест 7
import unittest

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from get_top_decade_ratings import get_top_decade_ratings


class TestGetTopDecadeRatings(unittest.TestCase):

    def test_get_top_decade_ratings(spark):
        # Create sample data
        ratings_data = [("tt0000001", 5.6), ("tt0000002", 6.5), ("tt0000003", 7.4)]
        basics_data = [("tt0000001", 1900, "movie", "Title1"), ("tt0000002", 1910, "tvSeries", "Title2"),
                       ("tt0000003", 1920, "movie", "Title3")]
        ratings_df = spark.createDataFrame(ratings_data, ["tconst", "averageRating"])
        basics_df = spark.createDataFrame(basics_data, ["tconst", "startYear", "titleType", "primaryTitle"])

        # Call the function
        output_path = "test_output"
        get_top_decade_ratings(ratings_df, basics_df, output_path)

        # Load the output
        result_df = spark.read.option("delimiter", "\t").csv(output_path, header=True, inferSchema=True)

        # Define expected results
        expected_data = [("1900", "Title1", 5.6, 1), ("1910", "Title2", 6.5, 1), ("1920", "Title3", 7.4, 1)]
        expected_df = spark.createDataFrame(expected_data, ["decade", "primaryTitle", "averageRating", "rank"])

        # Assert the results
        assert result_df.collect() == expected_df.collect()