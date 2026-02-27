"""
WEBAPI CODE

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

from pyspark.sql import SparkSession
from pyspark import  SparkContext,SparkConf


config=SparkConf().setAppName("webapi")
sc=SparkContext(conf=config)
spark=SparkSession.builder.getOrCreate()


import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WebAPI").getOrCreate()

url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(url)

data = response.json()   # list of dicts

df = spark.createDataFrame(data)
df.show(5,truncate=False)
df.printSchema()
