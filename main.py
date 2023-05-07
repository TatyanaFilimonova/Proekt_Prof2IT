import pyspark
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.functions import sum, col, desc, split, collect_list

from get_actor_roles_by_title import get_actor_roles_by_title
from get_long_movies import get_long_movies
from get_people_born_in_19th_century import get_people_born_in_19th_century
from get_top_adult_movies_region import get_top_adult_movies_region
from get_top_decade_ratings import get_top_decade_ratings
from get_top_genre_ratings import get_top_genre_ratings
from get_top_tv_series import get_top_tv_series
from spark import spark
from get_ukrainian_movies_and_tv_shows import get_ukrainian_movies_and_tv_shows

if __name__ == '__main__':

    # зчитати файли
    name_basics = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/name.basics.tsv.gz")

    title_akas = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.akas.tsv.gz")

    title_basics = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.basics.tsv.gz")

    title_crew = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.crew.tsv.gz")

    title_episode = spark.read.format("csv").option("sep", "\t").option("header", "true").load("data/title.episode.tsv.gz")

    title_principals = spark.read.option("header", "true").option("delimiter", "\t").csv("data/title.principals.tsv.gz")

    title_ratings = spark.read.option("header", "true").option("delimiter", "\t").csv("data/title.ratings.tsv.gz")

    # 1. Отримайте всі назви серіалів / фільмів тощо, які доступні українською мовою.
    # get_ukrainian_movies_and_tv_shows(title_akas, "ukrainian_movies_and_tv_shows.csv")

    # 2. Отримайте список імен людей, які народилися в 19 столітті.
    #get_people_born_in_19th_century(name_basics, "people_born_in_19th_century.csv")

    # 3. Отримайте назви всіх фільмів, які тривають понад 2 години.
    # get_long_movies(title_basics, "get_long_movies.csv")

    # 4. Отримайте імена людей, відповідні фільмам / серіалам та персонажі, які вони грали у цих фільмах.
    #get_actor_roles_by_title(name_basics, title_principals, title_akas, "get_actor_roles_by_title.csv")

    # 5. Отримайте інформацію про те, скільки дорослих фільмів / серіалів тощо є вкожному регіоні. Отримайте топ-100 з найбільшою кількістю з найбільшої кількості
    # в регіоні з найменшою.
    #get_top_adult_movies_region(title_akas, title_basics)


    # 6. Отримайте інформацію про те, скільки епізодів у кожному телесеріалі. Отримайте топ-50, починаючи з телесеріалу з найбільшою кількістю епізодів.
    # get_top_tv_series(title_basics, title_episode)


    #7. Отримайте 10 назв найпопулярніших фільмів / серіалів тощо за кожним десятиліттям.
    get_top_decade_ratings(title_ratings, title_basics, "get_top_decade_ratings.csv")
    """"

    # 8. Отримайте 10 назв найпопулярніших фільмів / серіалів тощо за кожним жанром.
    get_top_genre_ratings("data/title.basics.tsv.gz", "data/title.ratings.tsv.gz", "get_top_genre_ratings.csv") """
