# Тест 2
from get_people_born_in_19th_century import get_people_born_in_19th_century


class TestGetPeopleBornIn19thCentury(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("test").getOrCreate()

        # Створити тестові дані
        data = [("John Smith", "male", "1845"),
                ("Jane Doe", "female", "1870"),
                ("William Brown", "male", "1812"),
                ("Elizabeth Taylor", "female", "1867"),
                ("James Wilson", "male", "1890")]
        columns = ["primaryName", "gender", "birthYear"]
        self.test_df = self.spark.createDataFrame(data, columns)

    def test_get_people_born_in_19th_century(self):
        # Очікуваний результат
        expected = [("William Brown",), ("John Smith",), ("Elizabeth Taylor",)]
        expected_df = self.spark.createDataFrame(expected, ["primaryName"])

        # Отримати фактичний результат
        result_df = get_people_born_in_19th_century(self.test_df)

        # Перевірити, чи співпадають очікуваний та фактичний результат
        self.assertEqual(result_df.collect(), expected_df.collect())

#   def tearDown(self):
#      self.spark.stop()