import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when, datediff

os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
weather = [
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)
    ]
df = spark.createDataFrame(weather,["day_id", "temperature", "humidity"])
df1 = (df.withColumn(
    "date_diff",
    when((col("temperature") > 30), "true")
    .otherwise("false"))
    .withColumn("is_humid", when((col("temperature") > 30), "true")
    .otherwise("false")
))
df1.show()




