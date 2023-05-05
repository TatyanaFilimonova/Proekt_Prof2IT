# 6. Отримайте інформацію про те, скільки епізодів у кожному телесеріалі. Отримайте топ-50, починаючи з телесеріалу з найбільшою кількістю епізодів.
from pyspark.sql.functions import desc


def get_top_tv_series(basics_df, episode_df):
    # Відбір записів з жанром "series"
    series_df = basics_df.filter(basics_df["titleType"] == "tvSeries")

    # Об'єднання таблиць та агрегація по кількості епізодів
    episode_count_df = episode_df.join(series_df, episode_df["parentTconst"] == series_df["tconst"], "inner") \
        .groupBy("primaryTitle") \
        .agg(sum("episodeNumber").alias("episodeCount")) \
        .orderBy(desc("episodeCount")) \
        .limit(50)

    # Виведення результату
    episode_count_df.show()

    # Збереження результатів в CSV файл
    episode_count_df.write.csv("episode_count.csv", header=True)

    # Повернення результатів
    return episode_count_df
