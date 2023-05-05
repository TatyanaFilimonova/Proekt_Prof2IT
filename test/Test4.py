# Тест 4
import os
import unittest

from get_actor_roles_by_title import get_actor_roles_by_title


class TestGetActorRolesByTitle(unittest.TestCase):

    def test_get_actor_roles_by_title(spark):
        # Створюємо тимчасові файли для тестування
        name_data = [("nm0000001", "Fred Astaire"), ("nm0000002", "Lauren Bacall"), ("nm0000003", "Brigitte Bardot"),
                     ("nm0000004", "John Belushi")]
        name_columns = ["nconst", "primaryName"]
        name_df = spark.createDataFrame(name_data, name_columns)

        title_principals_data = [("tt0000001", "nm0000001", "actor"), ("tt0000001", "nm0000002", "actress"),
                                 ("tt0000002", "nm0000003", "actor"), ("tt0000002", "nm0000004", "actor")]
        title_principals_columns = ["tconst", "nconst", "category"]
        title_principals_file = "test_data/title.principals.tsv.gz"
        title_principals_df = spark.createDataFrame(title_principals_data, title_principals_columns)
        title_principals_df.write.csv(title_principals_file, mode='overwrite', header=True, sep='\t')

        title_akas_data = [("tt0000001", "Great Movie", "en"), ("tt0000002", "Another Great Movie", "en"),
                           ("tt0000002", "Encore un super film", "fr")]
        title_akas_columns = ["titleId", "title", "language"]
        title_akas_file = "test_data/title.akas.tsv.gz"
        title_akas_df = spark.createDataFrame(title_akas_data, title_akas_columns)
        title_akas_df.write.csv(title_akas_file, mode='overwrite', header=True, sep='\t')

        # Запускаємо функцію
        output_file = "test_output/actor_roles_by_title.csv"
        get_actor_roles_by_title(name_df, title_principals_file, title_akas_file, output_file)

        # Перевіряємо результати
        assert os.path.exists(output_file)
        with open(output_file, "r") as f:
            lines = f.readlines()
            assert len(lines) == 3  # треба врахувати заголовок
            assert lines[1].startswith("Astaire")  # перевіряємо перший запис

        # Видаляємо тимчасові файли
        os.remove(title_principals_file)
        os.remove(title_akas_file)
        os.remove(output_file)

    # def tearDown(self):
    # self.spark.stop()