"""
scenario : 3

+--------+----------+------+
|sensorid| timestamp|values|
+--------+----------+------+
|    1111|2021-01-15|    10|
|    1111|2021-01-16|    15|
|    1111|2021-01-17|    30|
|    1112|2021-01-15|    10|
|    1112|2021-01-15|    20|
|    1112|2021-01-15|    30|
+--------+----------+------+


ouput:

+--------+----------+------+
|sensorid| timestamp|values|
+--------+----------+------+
|    1111|2021-01-15|     5|
|    1111|2021-01-16|    15|
|    1112|2021-01-15|    10|
|    1112|2021-01-15|    10|
+--------+----------+------+

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


data = [(1111, "2021-01-15", 10),
        (1111, "2021-01-16", 15),
        (1111, "2021-01-17", 30),
        (1112, "2021-01-15", 10),
        (1112, "2021-01-15", 20),
        (1112, "2021-01-15", 30)]

myschema = ["sensorid", "timestamp", "values"]

df = spark.createDataFrame(data, schema=myschema)
df.show()

from pyspark.sql import Window

print("DSL")

window=Window.partitionBy("sensorid").orderBy("values")

df.withColumn("leadval",lead("values",1).over(window))\
    .filter(col("leadval").isNotNull())\
    .withColumn("values",col("leadval").cast(IntegerType())-col("values").cast(IntegerType()))\
    .drop("leadval").show()

print(" OTHERWAY as expr()")

df.withColumn("leadval",lead("values",1).over(window)) \
    .filter(col("leadval").isNotNull()) \
    .withColumn("values",expr(" leadval-values")).drop("leadval").show()

print("SPARK SQL")
df.createOrReplaceTempView("sqldf")

spark.sql(""" select sensorid ,timestamp,(leadvalue-values) as values from (
select * ,lead(values,1) over(PARTITION  by sensorid order by values ) as leadvalue from sqldf 
)t where leadvalue is not null
""").show()