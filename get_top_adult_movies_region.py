# 5. Отримайте інформацію про те, скільки дорослих фільмів / серіалів тощо є вкожному регіоні. Отримайте топ-100 з найбільшою кількістю з найбільшої кількості
# в регіоні з найменшою.

from pyspark.sql.functions import sum, when, col, count


def get_top_adult_movies_region(title_akas, title_basics):

    title_akas = title_akas.withColumnRenamed("titleId", "tconst")

    # Об'єднуємо датасети title_ratings та title_akas за допомогою ключа "tconst"
    ratings_region = title_basics.join(title_akas, "tconst").select("region", "isAdult")

    filtered_data = ratings_region.filter((col("isAdult") == 1) & (col("region") != "\\N"))

    # Select the top 100 regions and count the number of movies in each region
    top_regions = filtered_data.groupBy("region").agg(count("*").alias("movieCount")).orderBy(col("movieCount").desc()).limit(100)
    # Виводимо результат
    top_regions.show()

    # Збереження результатів в CSV файл
    top_regions.write.csv("max_adult_movies_regions.csv", mode="overwrite", header=True)

    # Повернення результатів
    return top_regions
