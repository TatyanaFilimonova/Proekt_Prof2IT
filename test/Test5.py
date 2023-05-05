import unittest

from pyspark.sql.connect.session import SparkSession

from get_top_adult_movies_region import get_top_adult_movies_region


class TestGetTopAdultMoviesRegion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Створення SparkSession для тестів
        cls.spark = SparkSession.builder.appName("test").getOrCreate()

    def test_max_adult_movies_regions_count(self):
        # Вхідні дані
        title_ratings_path = "path/to/title.ratings.tsv.gz"
        title_akas_path = "path/to/title.akas.tsv.gz"

        # Виклик функції
        result = get_top_adult_movies_region(title_ratings_path, title_akas_path)

        # Перевірка на кількість записів
        self.assertEqual(result.count(), 1)

        # Перевірка на правильність значення "adult_movies_count"
        self.assertEqual(result.first()["adult_movies_count"], 100)

    #@classmethod
    #def tearDownClass(cls):
        # Зупинка SparkSession
        #cls.spark.stop()