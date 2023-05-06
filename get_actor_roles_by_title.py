from pyspark.sql.functions import sum, col, desc, split, collect_list
from spark import spark


# 4. Отримайте імена людей, відповідні фільмам / серіалам та персонажі, які вони грали у цих фільмах.

def get_actor_roles_by_title(name_df, title_principals, title_akas, output_file):
    title_principals = title_principals.filter((title_principals.category == "actor") | (title_principals.category == "actress"))
    title_akas = title_akas.withColumnRenamed("titleId", "tconst")

    # Об'єднуємо датасети name_basics та title_principals за допомогою ключа "nconst"
    name_principals = name_df.join(title_principals, "nconst")

    # Об'єднуємо датасети name_principals та title_akas за допомогою ключа "tconst"
    title_actor_roles = name_principals.join(title_akas, "tconst")

    # Використовуємо функцію split, щоб розділити колонку "primaryName" на дві окремі колонки "firstName" та "lastName"
    title_actor_roles = title_actor_roles.withColumn("firstName", split(title_actor_roles.primaryName, ", ").getItem(1))
    title_actor_roles = title_actor_roles.withColumn("lastName", split(title_actor_roles.primaryName, ", ").getItem(0))

    # Відбираємо необхідні колонки
    title_actor_roles = title_actor_roles.select("lastName", "title", "characters")

    # Виводимо результат
    title_actor_roles.head()

    # Зберігаємо результат в CSV файл
    title_actor_roles.write.csv(output_file, mode='overwrite', header=True)

    # повертаємо результат
    return title_actor_roles
