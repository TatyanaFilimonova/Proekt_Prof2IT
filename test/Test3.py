# Тест 3
import unittest
from pyspark.sql import SparkSession

from get_long_movies import get_long_movies


class TestGetLongMovies(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("test").getOrCreate()

        # Створити тестові дані
        data = [("The Shawshank Redemption", 1994, 142),
                ("The Godfather", 1972, 175),
                ("The Dark Knight", 2008, 152),
                ("The Lord of the Rings: The Return of the King", 2003, 201),
                ("Interstellar", 2014, 169)]
        columns = ["title", "year", "duration"]
        self.test_df = self.spark.createDataFrame(data, columns)

    def test_get_long_movies(self):
        # Очікуваний результат
        expected = [("The Lord of the Rings: The Return of the King", 201)]
        expected_df = self.spark.createDataFrame(expected, ["title", "duration"])

        # Отримати фактичний результат
        result_df = get_long_movies(self.test_df, 120, "output.csv")

        # Перевірити, чи співпадають очікуваний та фактичний результат
        self.assertEqual(result_df.collect(), expected_df.collect())

# def tearDown(self):
#    self.spark.stop()
