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


simpleData = [("James",34,"2006-01-01","true","M",3000.60),
              ("Michael",33,"1980-01-10","true","F",3300.80),
              ("Robert",37,"06-01-1992","false","M",5000.50)
              ]

columns = ["firstname","age","jobStartDate","isGraduated","gender","salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

#converting age as int in DSL and SPARK SQL
#DSL
df.withColumn("age",col("age").cast(IntegerType())).printSchema()
print("expr >>>")
df.withColumn("age",expr(" cast(age as int)")).printSchema()

df.createOrReplaceTempView("sqldf")
spark.sql("select * ,int(age) from sqldf").printSchema()