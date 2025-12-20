from pyspark.sql import  SparkSession
from pyspark import SparkContext,SparkConf

import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

conf = SparkConf().setAppName("pyspark").setMaster("local[*]")

sc= SparkContext(conf=conf) # here conf=conf is important

# conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
# sc = SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

print("STARTED THE PYSPARK")


ls  =  [  1  ,  2   ,  3   ,  4  ]
print()
print("========rawList======")
print(ls)
rddls=sc.parallelize(ls)
print()
print("========rddls======")
print(rddls.collect())
addrdd  =  rddls.map(  lambda   x : x + 2)
print()
print("========addrdd======")
print(addrdd.collect())