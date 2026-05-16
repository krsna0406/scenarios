"""

Write a sql query to find manager atleast 3 direct report

assume:

input:
+------+----+----------+
|emp_id|name|manager_id|
+------+----+----------+
|     1|   A|      NULL|
|     2|   B|         1|
|     3|   C|         1|
|     4|   D|         1|
|     5|   E|         2|
|     6|   F|         2|
|     7|   G|         2|
|     8|   H|         2|
+------+----+----------+



output:
+------+----+
|emp_id|name|
+------+----+
|     1|   A|
|     2|   B|
+------+----+


imp note:

SQL:
SELECT manager_id
FROM employee
GROUP BY manager_id
HAVING COUNT(emp_id) >= 3;

If You Want Manager Names IMP ***

SELECT m.emp_id,
       m.name
FROM employee m
JOIN (
        SELECT manager_id
        FROM employee
        GROUP BY manager_id
        HAVING COUNT(*) >= 3
     ) t
ON m.emp_id = t.manager_id;






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
    (1,"A",None),
    (2,"B",1),
    (3,"C",1),
    (4,"D",1),
    (5,"E",2),
    (6,"F",2),
    (7,"G",2),
    (8,"H",2)
]

df = spark.createDataFrame(data, ["emp_id","name","manager_id"])
df.show()
#Step 2 — Find managers with ≥3 direct reports

mgr = df.groupBy("manager_id") \
    .agg(count("emp_id").alias("report_count")) \
    .filter(col("report_count") >= 3)

#Step 3 — Get Manager Details

result = df.join(mgr, df.emp_id == mgr.manager_id) \
    .select(df.emp_id, df.name)

result.show()


