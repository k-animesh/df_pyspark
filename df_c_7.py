import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when, datediff

os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
scores = [
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ]
df = spark.createDataFrame(scores,["student_id", "math_score", "english_score"])
df1 = df.withColumn("math_grade",when((col("math_score")>80),"A") \
      .when((col("math_score")>=60) & (col("math_score")<=80),"B") \
      .otherwise("C")) \
       .withColumn("english_grade",when((col("english_score")>80),"A") \
      .when((col("english_score")>=60) & (col("english_score")<=80),"B") \
      .otherwise("C"))

df1.show()




