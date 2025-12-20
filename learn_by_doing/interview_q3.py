"""
INTERVIEW QUESTION
1. how to combine many list

SOLVE USING PYSPARK AND SPARK SQL
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

list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

# create RDD

rdd=sc.parallelize( list(zip(list1,list2)) )
df=rdd.toDF(["column1","column2"])
df.show()