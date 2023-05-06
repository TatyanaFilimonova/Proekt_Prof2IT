# Тест 2
import unittest

from pyspark.sql import SparkSession

from get_people_born_in_19th_century import get_people_born_in_19th_century


class TestGetPeopleBornIn19thCentury(unittest.TestCase):

    def setUp(self):
        self.spark = (SparkSession
                      .builder
                      .master("local[*]")
                      .appName("Unit-tests")
                      .getOrCreate())
        # Створити тестові дані
        data = [("John Smith", "male", "1845"),
                ("Jane Doe", "female", "1970"),
                ("William Brown", "male", "1812"),
                ("Elizabeth Taylor", "female", "1867"),
                ("James Wilson", "male", "1990")]
        columns = ["primaryName", "gender", "birthYear"]
        self.test_df = self.spark.createDataFrame(data, columns)

    def tearDown(self):
        self.spark.stop()

    def test_get_people_born_in_19th_century(self):
        # Очікуваний результат
        expected = [("John Smith",), ("William Brown",), ("Elizabeth Taylor",)]
        expected_df = self.spark.createDataFrame(expected, ["primaryName"])

        # Отримати фактичний результат
        result_df = get_people_born_in_19th_century(self.test_df,  "test_output.csv")

        # Перевірити, чи співпадають очікуваний та фактичний результат
        self.assertEqual(result_df.collect(), expected_df.collect())
