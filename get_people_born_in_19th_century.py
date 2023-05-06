# 2. Отримайте список імен людей, які народилися в 19 столітті.
from pyspark.sql.functions import col
from spark import spark


def get_people_born_in_19th_century(name_df, output_file):
    # відфільтрувати за умовою
    born_in_19th_century = name_df.filter(col("birthYear").startswith("18"))

    # отримати список унікальних імен
    people_names = born_in_19th_century.select("primaryName").distinct()

    # записати результат у CSV файл
    people_names.write.csv(output_file, mode="overwrite", header=True)

    # виведемо результат
    people_names.show()

    # повернути результат
    return people_names