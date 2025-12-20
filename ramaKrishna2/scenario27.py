"""

scenario 27 :
input:

+-----+------+----+
|empid|salary|year|
+-----+------+----+
|    1| 60000|2018|
|    1| 70000|2019|
|    1| 80000|2020|
|    2| 60000|2018|
|    2| 65000|2019|
|    2| 65000|2020|
|    3| 60000|2018|
|    3| 65000|2019|
+-----+------+----+
expected :

+-----+------+----+-----------+
|empid|salary|year|incresalary|
+-----+------+----+-----------+
|    1| 60000|2018|          0|
|    1| 70000|2019|      10000|
|    1| 80000|2020|      10000|
|    2| 60000|2018|          0|
|    2| 65000|2019|       5000|
|    2| 65000|2020|          0|
|    3| 60000|2018|          0|
|    3| 65000|2019|       5000|
+-----+------+----+-----------+


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
data = [(1,60000,2018),(1,70000,2019),(1,80000,2020),(2,60000,2018),(2,65000,2019),(2,65000,2020),(3,60000,2018),(3,65000,2019)]

df = spark.createDataFrame(data,["empid","salary","year"])

df.show()

from pyspark.sql import Window

window=Window.partitionBy("empid").orderBy(col("year").asc())

windf=df.withColumn("prev_salary",lag("salary",1).over(window))
windf.withColumn("incresalary",expr(""" salary - prev_salary """))\
    .withColumn("incresalary",coalesce("incresalary",lit(0))).drop("prev_salary").show()