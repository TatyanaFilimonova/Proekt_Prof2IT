# Тест 1
import unittest
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
import unittest

from get_ukrainian_movies_and_tv_shows import get_ukrainian_movies_and_tv_shows


class TestGetUkrainianMoviesAndTvShows(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Unit-tests")
                      .getOrCreate())

        # створюємо тестовий DataFrame
        self.title_df = self.spark.createDataFrame(
            [(1, "Movie 1", "UA"),
             (2, "Movie 2", "US"),
             (3, "TV Show 1", "UA"),
             (4, "TV Show 2", "US"),
             (5, "Movie 3", "UA")],
            ["titleId", "title", "region"]
        )

    def tearDown(self):
        self.spark.stop()

    def test_get_ukrainian_movies_and_tv_shows(self):
        expected_result = [("Movie 1",), ("TV Show 1",), ("Movie 3",)]

        # викликаємо тестову функцію
        result_df = get_ukrainian_movies_and_tv_shows(self.title_df, "test_output.csv")

        # перевіряємо, чи повертається очікуваний результат
        self.assertListEqual(expected_result, result_df.collect())



