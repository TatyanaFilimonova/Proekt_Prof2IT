# Тест 6
import unittest

from pyspark.sql import SparkSession

from get_top_tv_series import get_top_tv_series

class TestTVSeries(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Unit-tests")
                      .getOrCreate())

    def tearDown(self):
        self.spark.stop()

    def test_get_top_tv_series(self):
        # create some test data
        basics_data = [("tt0001", "tvSeries", "The Show 1"),
                       ("tt0002", "movie", "The Movie 1"),
                       ("tt0003", "tvSeries", "The Show 2"),
                       ("tt0004", "tvSeries", "The Show 3")]
        basics_df = self.spark.createDataFrame(basics_data, ["tconst", "titleType", "originalTitle"])

        episode_data = [("tt0001", 1),
                        ("tt0001", 2),
                        ("tt0002", 1),
                        ("tt0003", 1),
                        ("tt0003", 2),
                        ("tt0004", 1),
                        ("tt0004", 2),
                        ("tt0004", 3),
                        ("tt0004", 4)]
        episode_df = self.spark.createDataFrame(episode_data, ["parentTconst", "episodeNumber"])

        # call the function
        result = get_top_tv_series(basics_df, episode_df)

        # check the output
        expected_output = [("The Show 3", 4), ("The Show 1", 2), ("The Show 2", 2)]
        self.assertEqual(result.collect(), expected_output)

