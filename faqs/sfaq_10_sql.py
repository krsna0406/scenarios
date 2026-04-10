"""
7.	SQL -  find the second highest salary in each department

SQL:
SQL Query

cgpt:

1. Using ROW_NUMBER() (Most common)
SELECT dept, empname, salary
FROM (
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY dept
               ORDER BY salary DESC
           ) AS rn
    FROM Employee
) t
WHERE rn = 2;
Logic
•	Partition by department
•	Sort salary descending
•	Pick row number 2
________________________________________
2. Using DENSE_RANK()
SELECT dept, empname, salary
FROM (
    SELECT *,
           DENSE_RANK() OVER(
               PARTITION BY dept
               ORDER BY salary DESC
           ) AS rnk
    FROM Employee
) t
WHERE rnk = 2;
Difference
Function	Handles duplicates
ROW_NUMBER	Unique sequence
DENSE_RANK	Same rank for same salary
________________________________________
3. Without Window Functions
SELECT dept, MAX(salary) AS second_highest_salary
FROM Employee e1
WHERE salary < (
    SELECT MAX(salary)
    FROM Employee e2
    WHERE e1.dept = e2.dept
)
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

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
