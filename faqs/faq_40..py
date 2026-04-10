"""
There are 3 columns in sql each column is having  null records

input:
+----+----+----+
|col1|col2|col3|
+----+----+----+
|  1 |NULL|  3 |
|NULL|  5 |NULL|
|  4 |NULL|NULL|
|NULL|  7 |  8 |
|  9 | 10 |NULL|
+----+----+----+

note:

1️⃣ Using SUM + CASE (most common)
SELECT
    SUM(CASE WHEN col1 IS NULL THEN 1 ELSE 0 END) AS col1_nulls,
    SUM(CASE WHEN col2 IS NULL THEN 1 ELSE 0 END) AS col2_nulls,
    SUM(CASE WHEN col3 IS NULL THEN 1 ELSE 0 END) AS col3_nulls
FROM emp;

Explanation

CASE WHEN col IS NULL THEN 1

SUM() adds them → gives total null count.

2️⃣ Using COUNT() trick (cleaner)

COUNT(column) ignores NULL values

SELECT
    COUNT(*) - COUNT(col1) AS col1_nulls,
    COUNT(*) - COUNT(col2) AS col2_nulls,
    COUNT(*) - COUNT(col3) AS col3_nulls
FROM emp;

Explanation

Total rows = COUNT(*)
Non-null rows = COUNT(col)
Null rows = difference

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
data = [
    (1, None, 3),
    (None, 5, None),
    (4, None, None),
    (None, 7, 8),
    (9, 10, None)
]

df = spark.createDataFrame(data, ["col1","col2","col3"])

df.show()

from pyspark.sql.functions import col, sum, when

df.select(
    sum(when(col("col1").isNull(),1).otherwise(0)).alias("col1_nulls"),
    sum(when(col("col2").isNull(),1).otherwise(0)).alias("col2_nulls"),
    sum(when(col("col3").isNull(),1).otherwise(0)).alias("col3_nulls")
).show()