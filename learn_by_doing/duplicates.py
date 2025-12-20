"""
DUPLICATES

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


data =[(1,'abc@gmail.com'),(2,'def@gmail.com'),(1,'abc@gmail.com')]
column=['id','name']

df =spark.createDataFrame(data,column)
df.show()

# BY USING SPARK DSL

df.groupby("id","name").agg(count("name").alias("count")).filter(col("count")>1).show()

#SPARK SQL

df.createOrReplaceTempView("sqldf")
spark.sql("select id,name from sqldf group by id,name having count(name) >1").show()