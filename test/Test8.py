# Test 8
import unittest

from pyspark.sql.functions import col
from pyspark.sql import SparkSession


from get_top_genre_ratings import get_top_genre_ratings


class TestGetTopGenreRatings(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Unit-tests")
                      .getOrCreate())

    def tearDown(self):
        self.spark.stop()

    def test_get_top_genre_ratings(self):
        # Задаємо вхідні параметри для функції
        title_basics = self.spark.read.format("csv").option("sep", "\t").option("header", "true").load("../data/title.basics.tsv.gz")
        title_ratings = self.spark.read.option("header", "true").option("delimiter", "\t").csv("../data/title.ratings.tsv.gz")

        output_path = "test_data/top_genre_ratings.csv"

        # Викликаємо функцію
        top_genre_ratings = get_top_genre_ratings(title_basics, title_ratings, output_path)

        # Очікувані результати для жанру "Action"
        expected_action = [("The Dark Knight", 1), ("Inception", 2)]

        # Отримуємо результати для жанру "Action"
        actual_action = top_genre_ratings.filter(col("genre") == "Action").select("primaryTitle", "rank").collect()[0:2]

        # Перевіряємо, чи повертає функція очікуваний результат для жанру "Action"
        self.assertEqual(expected_action, [(r[0], r[1]) for r in actual_action])

