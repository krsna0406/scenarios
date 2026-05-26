"""
df column to list

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


from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import *


conf=SparkConf().setAppName("col2Lsit").setMaster("local[*]")
sc= SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()
data = [
    (101, "John"),
    (102, "Mary"),
    (103, "David")
]
df = spark.createDataFrame(data, ["id", "name"])
df.show()

# print(df)
# print(type(df))


print('by using flatmap')
print(df.select(col("name")).rdd.flatMap(lambda x:x).collect())


print('by using map')

print(df.select(col("name")).rdd.map(lambda x: x[0]).collect())


print("list comprehension")
print([ row[0] for row in df.select(col("name")).collect()])

print("list comprehension by using col name ")

print([ row.name for row in df.select(col("name")).collect()])

