import pyspark
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, desc, split, collect_list

from get_actor_roles_by_title import get_actor_roles_by_title
from get_long_movies import get_long_movies
from get_people_born_in_19th_century import get_people_born_in_19th_century
from get_top_adult_movies_region import get_top_adult_movies_region
from get_top_decade_ratings import get_top_decade_ratings
from get_top_genre_ratings import get_top_genre_ratings
from get_top_tv_series import get_top_tv_series
from get_ukrainian_movies_and_tv_shows import get_ukrainian_movies_and_tv_shows

if __name__ == '__main__':

    spark = SparkSession.builder.appName("Read TSV file").getOrCreate()

    # зчитати файли
    name_df = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/name.basics.tsv.gz")
    name_df.show()

    title_df = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.akas.tsv.gz")
    title_df.show()

    title_crew_df = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.crew.tsv.gz")
    title_crew_df.show()

    title_principals = spark.read.option("header", "true").option("delimiter", "\t").csv("data/title.principals.tsv.gz")
    title_principals.show()

    episode_df = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.episode.tsv.gz")
    episode_df.show()

    basics_df = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.basics.tsv.gz")
    basics_df.show()

    raitings_df = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.ratings.tsv.gz")
    basics_df.show()

    df_6 = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.principals.tsv.gz")

    # 1. Отримайте всі назви серіалів / фільмів тощо, які доступні українською мовою.
    get_ukrainian_movies_and_tv_shows(title_df, "ukrainian_movies_and_tv_shows.csv")

    # 2. Отримайте список імен людей, які народилися в 19 столітті.
    get_people_born_in_19th_century(name_df, "people_born_in_19th_century.csv")

    # 3. Отримайте назви всіх фільмів, які тривають понад 2 години.
    get_long_movies(title_df, "get_long_movies.csv")

    # 4. Отримайте імена людей, відповідні фільмам / серіалам та персонажі, які вони грали у цих фільмах.
    get_actor_roles_by_title(name_df, "get_actor_roles_by_title.csv", spark)

    # 5. Отримайте інформацію про те, скільки дорослих фільмів / серіалів тощо є вкожному регіоні. Отримайте топ-100 з найбільшою кількістю з найбільшої кількості
    # в регіоні з найменшою.
    get_top_adult_movies_region("data/title.ratings.tsv.gz", "data/title.akas.tsv.gz", spark)

    # 6. Отримайте інформацію про те, скільки епізодів у кожному телесеріалі. Отримайте топ-50, починаючи з телесеріалу з найбільшою кількістю епізодів.
    get_top_tv_series(basics_df, episode_df)

    #7. Отримайте 10 назв найпопулярніших фільмів / серіалів тощо за кожним десятиліттям.
    get_top_decade_ratings("data/title.ratings.tsv.gz", "data/title.basics.tsv.gz", "get_top_decade_ratings.csv")

    # 8. Отримайте 10 назв найпопулярніших фільмів / серіалів тощо за кожним жанром.
    get_top_genre_ratings("data/title.basics.tsv.gz", "data/title.ratings.tsv.gz", "get_top_genre_ratings.csv")
