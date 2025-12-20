"""

scenario 34:

input:

+-----------+------+---+------+
|customer_id|  name|age|gender|
+-----------+------+---+------+
|          1| Alice| 25|     F|
|          2|   Bob| 40|     M|
|          3|   Raj| 46|     M|
|          4| Sekar| 66|     M|
|          5|  Jhon| 47|     M|
|          6|Timoty| 28|     M|
|          7|  Brad| 90|     M|
|          8|  Rita| 34|     F|
+-----------+------+---+------+
expected :

+---------+-----+
|age_group|count|
+---------+-----+
|    19-35|    3|
|    36-50|    3|
|      51+|    2|
+---------+-----+

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

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()

data = [(1,'Alice',25,'F'),(2,'Bob',40,'M'),(3,'Raj',46,'M'),(4,'Sekar',66,'M'),(5,'Jhon',47,'M'),(6,'Timoty',28,'M'),(7,'Brad',90,'M'),(8,'Rita',34,'F')]

df = spark.createDataFrame(data,['customer_id','name','age','gender'])
df.show()

df1=df.withColumn("age_group", when( ((col("age") >= 19) & (col("age") <=35)) ,"19-35" )\
    . when( ((col("age") >= 36) & (col("age") <=50)) ,"36-50" )\
              .otherwise("51+")  )

df1.groupby("age_group").agg(count(col("age_group")).alias("count")).show()