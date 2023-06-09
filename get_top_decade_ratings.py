# 7. Отримайте 10 назв найпопулярніших фільмів / серіалів тощо за кожним десятиліттям.
from pyspark.sql.functions import regexp_extract, floor, col, row_number, desc
from pyspark.sql.window import Window
from spark import spark


def get_top_decade_ratings(ratings_df, basics_df, output_path):

    # вибір потрібних стовпців
    ratings_df = ratings_df.select("tconst", "averageRating")
    basics_df = basics_df.select("tconst", "startYear", "titleType", "primaryTitle")

    # збіг датасетів по стовпцю tconst
    joined = ratings_df.join(basics_df, on="tconst")

    # вибір тільки фільмів і серіалів
    joined = joined.filter(col("titleType").isin(["movie", "tvSeries"]))

    # витягнення десятиліття
    joined = joined.withColumn("decade", floor(col("startYear") / 10) * 10)

    # вікно для ранжування за популярністю
    w = Window.partitionBy("decade").orderBy(col("averageRating").desc())

    # вибір топ-10 фільмів/серіалів за кожним десятиліттям
    top_10 = (joined.select("decade", "primaryTitle", "averageRating", row_number().over(w).alias("rank"))
              .filter(col("rank") <= 10)
              .orderBy("decade", "rank"))

    # збереження результату у CSV-файл
    top_10.write.option("header", "true").option("delimiter", "\t").csv(output_path, mode='overwrite')

    # виведення результату
    top_10.orderBy(desc("decade")).show()

    # повернення результату
    return top_10