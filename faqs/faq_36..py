"""
Refer below two tables, We have different schamas , How We can merge ?

+-----------------+---+
|             Name|Age|
+-----------------+---+
|Azarudeen, Shahul| 25|
|   Michel, Clarke| 26|
|     Virat, Kohli| 28|
|   Andrew, Simond| 37|
+-----------------+---+

+----------------+---+------+
|            Name|Age|Gender|
+----------------+---+------+
|Rabindra, Tagore| 32|  Male|
|   Madona, Laure| 59|Female|
| Flintoff, David| 12|  Male|
|    Ammie, James| 20|Female|
+----------------+---+------+

imp note:


"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql import Window


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
# Step 1 — Create DataFrames

data1 = [
    ("Azarudeen, Shahul",25),
    ("Michel, Clarke",26),
    ("Virat, Kohli",28),
    ("Andrew, Simond",37)
]

df1 = spark.createDataFrame(data1,["Name","Age"])

data2 = [
    ("Rabindra, Tagore",32,"Male"),
    ("Madona, Laure",59,"Female"),
    ("Flintoff, David",12,"Male"),
    ("Ammie, James",20,"Female")
]

df2 = spark.createDataFrame(data2,["Name","Age","Gender"])

df1.show()
df2.show()

df1.withColumn("Gender",lit("NA")).unionByName(df2).show()

#4. Better Production Method (Spark 3+)

# Instead of manually adding columns:

final_df = df1.unionByName(df2, allowMissingColumns=True)
final_df.show()

# Spark automatically fills missing columns with NULL.