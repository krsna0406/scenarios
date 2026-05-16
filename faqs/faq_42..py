"""
You are given a dataset containing daily stock prices.
Write a PySpark program to calculate the running total of stock prices
 for each stock symbol in the dataset

input:

+----------+------+-----+
|      date|symbol|price|
+----------+------+-----+
|2024-09-01|  AAPL|  150|
|2024-09-02|  AAPL|  160|
|2024-09-03|  AAPL|  170|
|2024-09-01| GOOGL| 1200|
|2024-09-02| GOOGL| 1250|
|2024-09-03| GOOGL| 1300|
+----------+------+-----+

output:

+----------+------+-----+-------------+
|      date|symbol|price|running_total|
+----------+------+-----+-------------+
|2024-09-01|  AAPL|  150|          150|
|2024-09-02|  AAPL|  160|          310|
|2024-09-03|  AAPL|  170|          480|
|2024-09-01| GOOGL| 1200|         1200|
|2024-09-02| GOOGL| 1250|         2450|
|2024-09-03| GOOGL| 1300|         3750|
+----------+------+-----+-------------+



note:
"""



# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql import Window


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
# Step 1 — Create DataFrames

data = [
    ("2024-09-01", "AAPL", 150),
    ("2024-09-02", "AAPL", 160),
    ("2024-09-03", "AAPL", 170),
    ("2024-09-01", "GOOGL", 1200),
    ("2024-09-02", "GOOGL", 1250),
    ("2024-09-03", "GOOGL", 1300)
]

df = spark.createDataFrame(data, ["date","symbol","price"])

df.show()
# 2) Define Window for Running Total

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

w = Window.partitionBy("symbol") \
    .orderBy("date") \
  #  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# 3) Calculate Running Total

print("result")

result = df.withColumn("running_total", sum("price").over(w))

result.show()
