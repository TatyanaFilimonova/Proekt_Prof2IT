# 5. Отримайте інформацію про те, скільки дорослих фільмів / серіалів тощо є вкожному регіоні. Отримайте топ-100 з найбільшою кількістю з найбільшої кількості
# в регіоні з найменшою.

from pyspark.sql.functions import sum, when


def get_top_adult_movies_region(title_ratings_path, title_akas_path, spark):
    # Завантажуємо датасет title.ratings.tsv.gz
    title_ratings = spark.read.option("header", "true").option("delimiter", "\t").csv(title_ratings_path)

    # Завантажуємо датасет title.akas.tsv.gz
    title_akas = spark.read.option("header", "true").option("delimiter", "\t").csv(title_akas_path).withColumnRenamed(
        "titleId", "tconst")

    # Об'єднуємо датасети title_ratings та title_akas за допомогою ключа "tconst"
    ratings_region = title_ratings.join(title_akas, "tconst").select("region", "averageRating", "numVotes")

    # Додаємо колонку "isAdult", щоб позначити дорослі фільми / серіали
    ratings_region = ratings_region.withColumn("isAdult", when(ratings_region.averageRating >= 18, 1).otherwise(0))

    # Групуємо за регіоном та сумуємо кількість дорослих фільмів / серіалів тощо
    adult_movies = ratings_region.groupBy("region").agg(sum("isAdult").alias("adult_movies_count"))

    # Сортуємо за спаданням та отримуємо топ-100 регіонів з найбільшою кількістю
    top_100_regions = adult_movies.orderBy(adult_movies.adult_movies_count.desc()).limit(100)

    # Знаходимо максимальну кількість дорослих фільмів / серіалів тощо в регіоні з найменшою кількістю
    max_adult_movies_count = top_100_regions.select("adult_movies_count").orderBy("adult_movies_count").first()[
        "adult_movies_count"]

    # Отримуємо регіони з максимальною кількістю дорослих фільмів / серіалів тощо
    max_adult_movies_regions = top_100_regions.filter(top_100_regions.adult_movies_count == max_adult_movies_count)

    # Виводимо результат
    max_adult_movies_regions.show()

    # Збереження результатів в CSV файл
    max_adult_movies_regions.write.csv("max_adult_movies_regions.csv", header=True)

    # Повернення результатів
    return max_adult_movies_regions
