import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when, datediff, month

os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
payments = [
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ]
df = spark.createDataFrame(payments,["payment_id", "payment_date"])
df1 = df.withColumn("quarter",when(month(col("payment_date")).isin(1,2,3),"Q1") \
      .when(month(col("payment_date")).isin(3,4,5),"Q2") \
      .when(month(col("payment_date")).isin(6,7,8),"Q3") \
      .otherwise("Q4")
    )

df1.show()




