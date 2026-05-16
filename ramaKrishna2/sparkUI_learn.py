"""

SPARK UI

A generic explanation won’t help you in interviews or real debugging—you need a job
where the DAG is non-trivial and clearly visible in Spark UI. Let’s use a realistic
PySpark pipeline that creates multiple stages, shuffles, and transformations so you can actually see DAG behavior.





"""

print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

from pyspark import SparkContext,SparkConf
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("DAG_Example").getOrCreate()

# Sample data
data = [
        ("India", "Mobile", 100, 2),
        ("India", "Laptop", 500, 1),
        ("India", "Mobile", 100, 3),
        ("US", "Mobile", 200, 2),
        ("US", "Laptop", 800, 1),
        ("US", "Tablet", 300, 4)
]

cols = ["country", "product", "price", "quantity"]

df = spark.createDataFrame(data, cols)

print("partition size---")
print(df.rdd.getNumPartitions())

# Step 1: Add revenue column (Narrow transformation)
df1 = df.withColumn("revenue", col("price") * col("quantity"))

# Step 2: Aggregate revenue per country and product (Wide transformation - shuffle)
df2 = df1.groupBy("country", "product").agg(sum("revenue").alias("total_revenue"))
print('no of partitions-df2-- ',df2.rdd.getNumPartitions())
# Step 3: Window function to rank products per country (Wide transformation)
windowSpec = Window.partitionBy("country").orderBy(col("total_revenue").desc())

df3 = df2.withColumn("rank", rank().over(windowSpec))
print('no of partitions df3 --- ',df3.rdd.getNumPartitions())
# Step 4: Filter top product per country
result = df3.filter(col("rank") == 1)

# Action (Triggers DAG execution)
result.show()

uinput=input("enter any no to exit")