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

from pyspark.sql.types import *

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()
data = [ ("36636","Finance",3000,"USA"),
         ("40288","Finance",5000,"IND"),
         ("42114","Sales",3900,"USA"),
         ("39192","Marketing",2500,"CAN"),
         ("34534","Sales",6500,"USA") ]
schema = StructType([
    StructField('id', StringType(), True),
    StructField('dept', StringType(), True),
    StructField('salary', IntegerType(), True),
    StructField('location', StringType(), True)
])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

#Convert scolumns to Map
from pyspark.sql.functions import col,lit,create_map
df = df.withColumn("propertiesMap",create_map(
    lit("salary"),col("salary"),
    lit("location"),col("location")
))
df.printSchema()
df.show(truncate=False)

df.printSchema()

df1=df.select("*",explode("propertiesMap").alias("map_key", "map_value"))
df1.printSchema()

df1.show()