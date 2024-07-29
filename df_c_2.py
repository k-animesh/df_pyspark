import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when
os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
reviews = [
      (1, 1),
      (2, 4),
      (3, 5)]

df = spark.createDataFrame(reviews,["review_id", "review"])
df1 = df.withColumn(
    "feedback",
    when(col("review") < 3, "Bad")
    .when((col("review") == 3) | (col("review") == 4), "Good")
    .otherwise("Excellent")
).withColumn(
    "is_positive",
    when(col("review") >= 3, "true").otherwise("false")
)


df1.show()




