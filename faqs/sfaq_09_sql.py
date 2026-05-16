"""
Table of employee
Col. - empid, empname, gender, dept
Output -
Col - dept, Total Male, Total Female


SQL:
SQL Query

cgpt:

Assume table:

Employee
--------
empid
empname
gender
dept

Example data

empid	empname	gender	dept
1	    Amit	    M	IT
2	    Riya	    F	IT
3	    John	    M	HR
4	    Sara	    F	HR
5	    Raj	        M	IT

Expected Output

dept	Total_Male	Total_Female
IT	    2	        1
HR	    1	        1
SQL Solution (Conditional Aggregation)
SELECT
    dept,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS Total_Male,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS Total_Female
FROM Employee
GROUP BY dept;
Logic

CASE WHEN gender='M' → counts males

CASE WHEN gender='F' → counts females

GROUP BY dept → groups department-wise.



Interview Tip

Another SQL solution using PIVOT:

SELECT *
FROM (
    SELECT dept, gender FROM Employee
) src
PIVOT (
    COUNT(gender)
    FOR gender IN ('M' AS Total_Male, 'F' AS Total_Female)
) p;


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

data = [
    (1,"Amit","M","IT"),
    (2,"Riya","F","IT"),
    (3,"John","M","HR"),
    (4,"Sara","F","HR"),
    (5,"Raj","M","IT")
]

cols = ["empid","empname","gender","dept"]
df = spark.createDataFrame(data,cols)

df.show()

from pyspark.sql.functions import sum, when, col

df.groupBy("dept").agg(
    sum(when(col("gender")=="M",1).otherwise(0)).alias("Total_Male"),
    sum(when(col("gender")=="F",1).otherwise(0)).alias("Total_Female")
).show()

