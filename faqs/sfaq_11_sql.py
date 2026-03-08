"""
Using single table, find employees who get more salary than managers

SQL:
SQL Query

SELECT e.empid, e.empname, e.salary
FROM Employee e
JOIN Employee m
ON e.manager_id = m.empid
WHERE e.salary > m.salary;


cgpt:
Assume a single employee table where each employee has a manager.

Table
Employee
--------
empid
empname
salary
manager_id

Example data

empid	empname	salary	manager_id
1	John	5000	3
2	Sara	7000	3
3	Mike	6000	NULL
4	David	8000	3

Here Mike (id=3) is the manager.

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

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
