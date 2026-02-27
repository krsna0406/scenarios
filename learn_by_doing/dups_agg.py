"""

INTERVIEW QUESTION
1. removing duplicates without using distinct
"""

print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, StructField

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
from pyspark.sql.functions import *


# data = [("Alice",), ("Bob",), ("Alice",), ("Charlie",)]
# df = spark.createDataFrame(data, ["name"])
#
# df.show()
#
# df.groupby("name").agg(first("name").alias("new_name")).drop("name").show()

#
# data = [("0",), ("1",), ("2",), ("10",),("4",),("5",)]
# df = spark.createDataFrame(data, ["number"])
#
# df.show()
#
#
# from pyspark.sql.window import Window
#
# window=Window.orderBy(col("number").asc())
#
# df.withColumn("lead", col("number")+lead("number").over(window)).show()

#
# data=[{"Trans_id" : "T1", "cust_id:" "123","trans_date": "27th June 2025"},
# {"Trans_id" : "T2", "cust_id:" "123","trans_date": "28th June 2025"},
# {"Trans_id" : "T3", "cust_id:" "123","trans_date": "30th June 2025"},
# {"Trans_id" : "T4", "cust_id:" "123","trans_date": "29th June 2025"},
# {"Trans_id" : "T5", "cust_id:" "123","trans_date": "29th June 2025"},
# {"Trans_id" : "T6", "cust_id:" "123","trans_date": "3rd July 2025"},
# {"Trans_id" : "T7", "cust_id:" "123","trans_date": "4rd July 2025"},
# {"Trans_id" : "T8", "cust_id:" "123","trans_date": "5th july 2025"}]
#
#
# df=spark.createDataFrame(data)
# df.show()
#



