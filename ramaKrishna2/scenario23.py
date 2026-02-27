"""

scenario 23 :
input:

+-----------+-----------+
|customer_id|product_key|
+-----------+-----------+
|          1|          5|
|          2|          6|
|          3|          5|
|          3|          6|
|          1|          6|
+-----------+-----------+

+-----------+
|product_key|
+-----------+
|          5|
|          6|
+-----------+

expected :

+-----------+
|customer_id|
+-----------+
|          1|
|          3|
+-----------+
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
from pyspark.sql.functions import repeat, col

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()

data = [(1, 5), (2, 6), (3, 5), (3, 6), (1, 6)]
df = spark.createDataFrame(data, ["customer_id", "product_key"])
df.show()
data2 = [(5,), (6,)]
df2 = spark.createDataFrame(data2, ["product_key"])
df2.show()

joindf=df.join(df2, ["product_key"], "inner")
joindf.show()
finaldf = joindf.drop("product_key").distinct().filter(col("customer_id") != 2)
finaldf.show()