"""
# DDL schema string
schema_ddl = "name STRING, age INT, salary DOUBLE"

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

# conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
# sc=SparkContext(conf=conf)

spark=SparkSession.builder\
    .config("spark.sql.shuffle.partitions",3)\
    .config("spark.sql.adaptive.enabled","false")\
    .getOrCreate()

data = [
    ("Alice", 25, 50000.0),
    ("Bob", 30, 60000.0),
    ("Charlie", 35, 70000.0)
]

# DDL schema string
schema_ddl = "name STRING, age INT, salary DOUBLE"

df = spark.createDataFrame(data, schema=schema_ddl)

df.show()
df.printSchema()