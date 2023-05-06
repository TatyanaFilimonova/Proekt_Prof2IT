
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read TSV file").getOrCreate()

