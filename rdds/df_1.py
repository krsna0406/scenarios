"""
DF faqs
"""

print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, StructField

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

# DF  operations
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import *
config=SparkConf().setAppName("aName").setMaster("local[*]")
sc=SparkContext(conf=config)

spark=SparkSession.builder.getOrCreate()
# 1. Create DataFrame from List (Most Basic)

data=[(1,'emp1',1000),(2,'emp2',2000),(1,'emp3',3000)]
schema=['empid','empname','sal']

schema1=StructType([StructField("empid", StringType(), True), \
                   StructField("empname", StringType(), True), \
                   StructField("salary", StringType(), True)])

# schema1=StructType([StructField()])
df=spark.createDataFrame(data,schema1)

df.show()
df.printSchema()

df.withColumn("sal",col("salary").cast('int')).printSchema()


rdd=sc.parallelize([(1,),(1,),(1,),(1,)])

rdd.toDF(["no"]).show()

df = spark.createDataFrame(
    [("Alice", 30)],
    "name STRING, age INT"
)
df.show()

# print("empty data frame")
# empty_df = spark.createDataFrame([], ["no"])
# empty_df.show()


print("empty data set")
df=spark.range(1, 10)
df.show()


print("11111111111111")

data = [(1, "Alice"), (2, "Bob")]
df = spark.createDataFrame(data)
df.show()



print(" by using named tuple")
from collections import namedtuple

Person= namedtuple('Person',['name','gender'])

data=[Person('a','M'),Person('B','F')]

ndf=spark.createDataFrame(data)
ndf.show()