"""

scenario 19 : FLATTENNING DATA

input:
expected :

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
from pyspark.sql.functions import repeat, col

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()

df= spark.read.format("json").option("multiline","true").load("C:\\app\\zeyoplus\\practice\\mKrishna2\\Datasets\\scen.json")

df.show(truncate=False)


df.printSchema()

df1=df.withColumn("dislikes",expr("likeDislike.dislikes"))\
              .withColumn("likes",expr("likeDislike.likes")) \
              .withColumn("userAction",expr("likeDislike.userAction")) .drop("likeDislike")\

df1.show(truncate=False)
df1.printSchema()

df2=df1.withColumn("multiMedia",expr("explode(multiMedia)"))
df2.show(truncate=False)
df2.printSchema()


df3=df2.select("*","multiMedia.*").drop("multiMedia")
df3.show(truncate=False)
df3.printSchema()
