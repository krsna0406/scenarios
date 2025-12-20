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

conf=SparkConf().setAppName("NULL HANDLING").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()


df=spark.read.csv("sample.csv",header=True,inferSchema=True)
df.show()
df.printSchema()
print("====INTEGER====")
df2=df.na.fill(0)
df2.show()
print("====STRING====")  # replace all non-numeric values with 0,important
df3=df.na.fill("NA").withColumn("type",col("type").cast(IntegerType())).\
    fillna(0,["type"])
df3.show()





