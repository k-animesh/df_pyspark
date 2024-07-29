import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col, when
os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"

spark = SparkSession.builder.appName("df_c_1").master("local[*]").getOrCreate()
documents = [
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ]

df = spark.createDataFrame(documents,["doc_id", "content"])
df1 = df.withColumn("content_category" ,when(col("content").contains ("fox") ,"Animal Related") \
      .when(col("content").contains("Lorem"), "Placeholder Text") \
      .otherwise("Tech Related")
    )


df1.show()




