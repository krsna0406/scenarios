"""

scenario 36:
input:
+----------+----------+
| sell_date|   product|
+----------+----------+
|2020-05-30| Headphone|
|2020-06-01|    Pencil|
|2020-06-02|      Mask|
|2020-05-30|Basketball|
|2020-06-01|      Book|
|2020-06-02|      Mask|
|2020-05-30|   T-Shirt|
+----------+----------+
expected :
+----------+--------------------+---------+
| sell_date|            products|null_sell|
+----------+--------------------+---------+
|2020-05-30|[T-Shirt, Basketb...|        3|
|2020-06-01|      [Pencil, Book]|        2|
|2020-06-02|              [Mask]|        1|
+----------+--------------------+---------+

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

data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]
columns = ["sell_date",'product']

df = spark.createDataFrame(data,schema=columns)
df.show()
# *** imp size()
df.groupby("sell_date").agg( collect_set(col("product")).alias("products"),size(collect_set(col("product"))).alias("null_sell")).show(truncate=False)
