"""
INTERVIEW QUESTION
1 question
1. Find out cummulative sales or running total sales
2. find out the prev sales
3. find out the nex sales
SOLVE USING PYSPARK AND SPARK SQL

input:

+----------+------+
|      date| sales|
+----------+------+
|2024-01-01| 20000|
|2024-01-02| 10000|
|2024-01-03|150000|
|2024-01-04|100000|
|2024-01-05|210000|
+----------+------+

output:

+----------+------+----------------+------------------+--------------+
|      date| sales|cumulative_sales|previous_mon_sales|next_mon_sales|
+----------+------+----------------+------------------+--------------+
|2024-01-01| 20000|           20000|                 0|         10000|
|2024-01-02| 10000|           30000|             20000|        150000|
|2024-01-03|150000|          180000|             10000|        100000|
|2024-01-04|100000|          280000|            150000|        210000|
|2024-01-05|210000|          490000|            100000|             0|
+----------+------+----------------+------------------+--------------+



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


data = [ ['2024-01-01',20000], ['2024-01-02',10000],[ '2024-01-03',150000], ['2024-01-04',100000], ['2024-01-05',210000]]

#define column names
columns = ['date', 'sales']

#create dataframe using data and column names
df = spark.createDataFrame(data, columns)
df.show()

window=Window.orderBy("date")
df1=(
        df.withColumn("pevious_mon_sales",lag("sales").over(window))
        .withColumn("next_mon_sales",lead("sales").over(window))
        .withColumn("running_total_sales",sum("sales").over(window))
        ).na.fill(0)

df1.show()


# by using SPARK SQL


df.createOrReplaceTempView("sqldf")

print("=======schema====")
df.printSchema()

spark.sql(""" select *,
        sum(sales) over(order by date) as cumulative_sales,
        lag(sales) over(order by date) as previous_mon_sales,
        lead(sales) over(order by date) as next_mon_sales from sqldf """).na.fill(0).show()