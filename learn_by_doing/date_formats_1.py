"""

INTERVIEW QUESTION
1. Find out the missing number
SOLVE USING PYSPARK AND SPARK SQL

"""
import pyspark

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

from pyspark import SparkContext,SparkConf
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
from pyspark.sql.functions import *



data = [("2022-03-15", "2022-03-16 12:34:56.789"),
        ("2022-03-01", "2022-03-16 01:23:45.678")]
df = spark.createDataFrame(data, ["date_col", "timestamp_col"])
df.show()
df.printSchema()

# now changing the formats

df.select("date_col",date_format("date_col","yyyy/MM/dd").alias("date")).show()

