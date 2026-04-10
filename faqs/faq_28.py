"""
Sum of salaries of all the employees department wise from Employee table
without using Partitions – Pyspark code
input:


imp note:
Goal: Calculate sum of salaries department-wise without using window partitions.
We simply use groupBy aggregation.

SQL:

SELECT dept, SUM(salary)
FROM employee
GROUP BY dept;


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


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()


data = [
    (1,"A","IT",5000),
    (2,"B","IT",7000),
    (3,"C","HR",6000),
    (4,"D","HR",4000),
    (5,"E","FIN",8000)
]

cols = ["emp_id","name","dept","salary"]

df = spark.createDataFrame(data, cols)

df.show()

from pyspark.sql.functions import sum

result = df.groupBy("dept") \
    .agg(sum("salary").alias("total_salary"))

result.show()