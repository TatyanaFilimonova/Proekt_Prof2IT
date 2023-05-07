# 8. Отримайте 10 назв найпопулярніших фільмів / серіалів тощо за кожним жанром.

from pyspark.sql.functions import col, desc, dense_rank, avg, explode, split
from pyspark.sql.window import Window
from spark import spark


def get_top_genre_ratings(title_basics, title_ratings, output_path):

    # Об'єднуємо датасети title_basics та title_ratings за допомогою ключа "tconst"
    title_genre_ratings = title_basics.join(title_ratings, "tconst")

    # Відбираємо необхідні колонки
    title_genre_ratings = title_genre_ratings.select("tconst", "primaryTitle", "genres", "averageRating", "numVotes")

    # Розбиваємо колонку "genres" на окремі жанри та використовуємо функцію explode для створення окремого рядка для кожного жанру
    title_genre_ratings = title_genre_ratings.withColumn("genre", explode(split(col("genres"), ",")))

    # Визначаємо рейтинг кожного фільму / серіалу як середнє значення рейтингу та кількості голосів
    title_genre_ratings = title_genre_ratings.withColumn("rating", col("averageRating") * col("numVotes"))

    # Визначаємо ранг кожного фільму / серіалу за жанром на основі рейтингу
    window = Window.partitionBy("genre").orderBy(desc("rating"), "primaryTitle")
    ranked_genre_ratings = title_genre_ratings.select("genre", "primaryTitle", dense_rank().over(window).alias("rank"),
                                                      "rating")

    # Відбираємо перші 10 найпопулярніших фільмів / серіалів за кожним жанром
    top_genre_ratings = ranked_genre_ratings.filter(col("rank") <= 10).orderBy("genre", "rank")
    top_genre_ratings.show()

    # Зберігаємо результати в CSV-файл
    top_genre_ratings.write.option("header", "true").option("delimiter", "\t").csv(output_path, mode="overwrite")

    # виведемо результат
    top_genre_ratings.show()

    # Повернення результатів
    return top_genre_ratings