"""



imput:


output:

notes:




"""

# print(__doc__)

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

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

data = [
    (1, "John", 5000),
    (2, "Sam", 6000)
]

df = spark.createDataFrame(data, ["Emp ID", "Emp Name", "Salary"])

df.show()

from pyspark.sql.functions import *

print(" DATE AND TIME RELATED ")
# 1. Current Date / Timestamp
dtdf=df.withColumn('today',current_date())\
    .withColumn('now', current_timestamp())

dtdf.show(truncate=False)
dtdf.printSchema()

# # 2. String → Date Conversion
# to_date()
#
# Converts string to DateType.

from pyspark.sql.functions import to_date

df = spark.createDataFrame([
    ("2026-05-13",)
], ["dt"])

# df.select(
#     to_date("dt").alias("date_col")
# ).printSchema()



#custom format

# to_date("dt", "dd-MM-yyyy")

df.select(
    to_date("dt", "dd-MM-yyyy").alias("date")
).show()

print("trunc")

df.select(
    trunc("dt", "month"),
    trunc("dt", "year")
).show()