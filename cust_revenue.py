import os
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col
os.environ["PYSPARK_PYTHON"] = "C:/Users/anime/Documents/Python/Python37/python.exe"
sc = SparkContext("local[*]", "cust_revenue")
orders = [
      (1, 101, "2023-07-01", 50.0),
      (2, 102, "2023-07-02", 30.0),
      (3, 101, "2023-07-03", 20.0),
      (4, 103, "2023-07-04", 70.0),
      (5, 102, "2023-07-05", 60.0)
    ]
rdd = sc.parallelize(orders)
rdd1 = rdd.map( lambda x : (x[1],x[3]))
rdd2 = rdd1.reduceByKey(lambda x,y: x+y)
rdd3 = rdd2.collect()
for i in rdd3:
    print(i)