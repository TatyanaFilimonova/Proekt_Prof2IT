# Test 8
import unittest

from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import col

from get_top_genre_ratings import get_top_genre_ratings


class TestGetTopGenreRatings(unittest.TestCase):

    def test_get_top_genre_ratings(self):
        # Задаємо вхідні параметри для функції
        title_basics_path = "test_data/title_basics.csv"
        title_ratings_path = "test_data/title_ratings.csv"
        output_path = "test_data/top_genre_ratings.csv"

        spark = SparkSession.builder.appName("Read TSV file").getOrCreate()

        # Викликаємо функцію
        top_genre_ratings = get_top_genre_ratings(title_basics_path, title_ratings_path, output_path, spark)

        # Очікувані результати для жанру "Action"
        expected_action = [("Avengers: Infinity War", 1), ("Avengers: Endgame", 2)]

        # Отримуємо результати для жанру "Action"
        actual_action = top_genre_ratings.filter(col("genre") == "Action").select("primaryTitle", "rank").collect()

        # Перевіряємо, чи повертає функція очікуваний результат для жанру "Action"
        self.assertEqual(expected_action, [(r[0], r[1]) for r in actual_action])

        # Очікувані результати для жанру "Comedy"
        expected_comedy = [("Friends", 1), ("The Office", 2)]

        # Отримуємо результати для жанру "Comedy"
        actual_comedy = top_genre_ratings.filter(col("genre") == "Comedy").select("primaryTitle", "rank").collect()

        # Перевіряємо, чи повертає функція очікуваний результат для жанру "Comedy"
        self.assertEqual(expected_comedy, [(r[0], r[1]) for r in actual_comedy])

        # Очікувані результати для жанру "Drama"
        expected_drama = [("Breaking Bad", 1), ("The Shawshank Redemption", 2)]

        # Отримуємо результати для жанру "Drama"
        actual_drama = top_genre_ratings.filter(col("genre") == "Drama").select("primaryTitle", "rank").collect()

        # Перевіряємо, чи повертає функція очікуваний результат для жанру "Drama"
        self.assertEqual(expected_drama, [(r[0], r[1]) for r in actual_drama])