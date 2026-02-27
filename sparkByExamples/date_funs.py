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

schema = StructType([
    StructField("input_timestamp", StringType(), True)])

dates = ['2019-07-01 12:01:19.111',
         '2019-06-24 12:01:19.222',
         '2019-11-16 16:44:55.406',
         '2019-11-16 16:50:59.406']

df = spark.createDataFrame(list( zip(dates)), schema=schema)

df.show(truncate=False)

df.printSchema()

df.withColumn("dateformat",date_format("input_timestamp",'yyyy/MM/dd"')).show()
