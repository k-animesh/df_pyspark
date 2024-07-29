import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when, datediff

os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
orders = [
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ]
df = spark.createDataFrame(orders,["order_id", "quantity", "total_price"])
df1 = df.withColumn(
    "date_diff",
    when((col("quantity") < 10) & (col("total_price") < 200), "Small&cheap")
    .when((col("quantity") >= 10)  & (col("total_price") < 200), "Bulk&Discounted")
    .otherwise("PremiumOrder")
)
df1.show()




