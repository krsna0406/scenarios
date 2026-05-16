"""
[A, b, g, d, &, @, ₹, %, 1, 2, 3, 4]
Given a list of characters in PySpark, how do you separate them into alphabets, numbers, and special characters and print each category with its count?


input:

+----+
|char|
+----+
|   A|
|   b|
|   g|
|   d|
|   &|
|   @|
|   ₹|
|   %|
|   1|
|   2|
|   3|
|   4|
+----+


output :

+-----------------+-----+
|         category|count|
+-----------------+-----+
|         alphabet|    4|
|Special Character|    4|
|           number|    4|
+-----------------+-----+


imp note:



"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, expr

# Initialize Spark
spark = SparkSession.builder.appName("DropColumnsWithNulls").getOrCreate()

data = [("A",),("b",),("g",),("d",),("&",),("@",),("₹",),("%",),("1",),("2",),("3",),("4",)]

df = spark.createDataFrame(data, ["char"])

df.show()

from pyspark.sql.functions import col
finaldf = df.withColumn(
    "category",
    expr("""
        case
            when char rlike '^[0-9]$' THEN 'number'
            when char rlike '^[A-Za-z]$' THEN 'alphabet'
            else 'Special Character'
        end
    """)
).groupBy("category").count()

finaldf.show()