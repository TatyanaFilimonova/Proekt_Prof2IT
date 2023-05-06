# 3. Отримайте назви всіх фільмів, які тривають понад 2 години.
from pyspark.sql.functions import col


def get_long_movies(title_df, output_file):
    # фільтруємо фільми, які тривають понад duration_threshold годин
    title_df_filtered = title_df.filter(col("runtimeMinutes") > 120)

    # вибираємо назви фільмів та їх тривалість
    title_df_result = title_df_filtered.select(col("originalTitle"), col("runtimeMinutes"))

    # зберігаємо результат у csv файлі
    title_df_result.write.format("csv").mode("overwrite").option("header", "true").save(output_file)

    # виведемо результат
    title_df_result.show()

    # повертаємо результат
    return title_df_result
