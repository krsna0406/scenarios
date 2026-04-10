"""
A CSV file contains 1000 records. Read the file from 5th row to last row.

input:


output:

imp note:

SQL:

"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql import  Window
spark = SparkSession.builder.appName("ex").getOrCreate()

df = spark.read.csv("file.csv", header=True, inferSchema=True)

window = Window.orderBy("id")

df2 = df.withColumn("row_num", row_number().over(window))

result = df2.filter("row_num >= 5").drop("row_num")

result.show()
