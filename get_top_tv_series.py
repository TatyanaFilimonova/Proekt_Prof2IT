# 6. Отримайте інформацію про те, скільки епізодів у кожному телесеріалі. Отримайте топ-50, починаючи з телесеріалу з найбільшою кількістю епізодів.
from pyspark.sql.functions import desc, col


def get_top_tv_series(basics_df, episode_df):
    # Відбір записів з жанром "series"

    filtered_data = basics_df.filter(col("titleType") == "tvSeries")

    joined_data = filtered_data.join(episode_df, filtered_data["tconst"] == episode_df["parentTconst"])

    joined_data.show()


    episode_count = joined_data.groupBy("originalTitle").count()


    # Sort the result in descending order by episode count
    sorted_episode_count = episode_count.orderBy(col("count").desc()).limit(50)


    # Збереження результатів в CSV файл
    sorted_episode_count.write.csv("episode_count.csv",  mode="overwrite", header=True)

    # Повернення результатів
    return sorted_episode_count
