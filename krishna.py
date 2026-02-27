"""
checking the SPARK UI and DAG etc
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


conf=SparkConf().setAppName("DAG").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()


df=spark.read.format("csv").option("header","true").load("C:\\Users\\Krishna\\IdeaProjects\\kanna\\df.csv")
df.show()

df.filter("category=='Exercise'").show()


input=input("please enter any to exit the sessions")