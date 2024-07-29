import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when, datediff

os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
emails = [
      (1, "user@gmail.com"),
      (2, "admin@yahoo.com"),
      (3, "info@hotmail.com")
    ]
df = spark.createDataFrame(emails,["email_id", "email_add"])
df1 = df.withColumn("email_domain", when(col("email_add").contains("gmail"),"GMAIL") \
      .when(col("email_add").contains("yahoo"),"YAHOO") \
      .when(col("email_add").contains("hotmail"),"HOTMAIL") \
    )

df1.show()




