from pyspark.sql.functions import col


def get_ukrainian_movies_and_tv_shows(title_df, output_file):
    ukrainian_movies_and_tv_shows = title_df.filter(col("region") == "UA").select("title")

    # Записати результати в файл
    ukrainian_movies_and_tv_shows.write.mode('overwrite').csv(output_file, header=True, mode="overwrite")

    # виведемо результат
    ukrainian_movies_and_tv_shows.show()

    # повернути результат
    return ukrainian_movies_and_tv_shows