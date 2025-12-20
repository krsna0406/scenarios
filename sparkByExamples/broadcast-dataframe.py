"""
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

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()

states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = sc.broadcast(states)


data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")
        ]

columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)


print("getting the broadcasting value")
#get the broadcast variable
print(broadcastStates.value)

print(df.rdd.map(lambda x: (x[0],x[1],x[2],x[3])))

def getheDictVale(key):
    return broadcastStates.value.get(key)



df.rdd.map(lambda x: (x[0],x[1],x[2],getheDictVale(x[3]))).toDF(columns).show()
