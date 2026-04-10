"""
find employees whose salary is greater than their manager's salary

input:
+------+----+----------+------+
|emp_id|name|manager_id|salary|
+------+----+----------+------+
|     1|   A|      NULL| 10000|
|     2|   B|         1|  6000|
|     3|   C|         1| 12000|
|     4|   D|         2|  7000|
|     5|   E|         2|  5000|
+------+----+----------+------+


output:

imp note:

SQL:

"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql import  Window
spark = SparkSession.builder.appName("ex").getOrCreate()
data = [
    (1, "A", None, 10000),
    (2, "B", 1, 6000),
    (3, "C", 1, 12000),
    (4, "D", 2, 7000),
    (5, "E", 2, 5000)
]

columns = ["emp_id","name","manager_id","salary"]

df = spark.createDataFrame(data, columns)

df.show()

from pyspark.sql.functions import col

emp = df.alias("emp")
mgr = df.alias("mgr")

result = emp.join(
    mgr,
    col("emp.manager_id") == col("mgr.emp_id"),
    "inner"
).filter(
    col("emp.salary") > col("mgr.salary")
).select(
    col("emp.emp_id"),
    col("emp.name"),
    col("emp.salary").alias("emp_salary"),
    col("mgr.name").alias("manager_name"),
    col("mgr.salary").alias("manager_salary")
)

result.show()

