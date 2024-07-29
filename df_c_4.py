import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when, datediff

os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
tasks = [
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ]

df = spark.createDataFrame(tasks,["task_id", "start_date", "end_date"])
df1 = df.withColumn(
    "date_diff",
    when(datediff(col("end_date"), col("start_date")) < 7, "short")
    .when((datediff(col("end_date"), col("start_date")) >= 7) & (datediff(col("end_date"), col("start_date")) <= 14), "medium")
    .otherwise("long")
)
df1.show()




