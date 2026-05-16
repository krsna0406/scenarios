"""
I have emp table with emp id item no and week and salary In the output
i need cumulative salary for 2 week as well as for 4 week

imp note:


You can compute rolling (cumulative) salary windows using SQL window functions.

Assume table emp

emp_id	item_no	week	salary

Goal:

2-week cumulative salary

4-week cumulative salary

SQL Query
SELECT
    emp_id,
    item_no,
    week,
    salary,

    SUM(salary) OVER (
        PARTITION BY emp_id, item_no
        ORDER BY week
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS salary_last_2_weeks,

    SUM(salary) OVER (
        PARTITION BY emp_id, item_no
        ORDER BY week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS salary_last_4_weeks

FROM emp;
Logic
2-Week Window
ROWS BETWEEN 1 PRECEDING AND CURRENT ROW

Example

week	salary	2-week sum
1	        100	100
2	        200	300
3	        300	500
4	        400	700
4-Week Window
ROWS BETWEEN 3 PRECEDING AND CURRENT ROW

Example

week	salary	4-week sum
1	100	100
2	200	300
3	300	600
4	400	1000
Key Points

PARTITION BY emp_id, item_no → compute separately per employee and item

ORDER BY week → ensures chronological calculation

ROWS BETWEEN → defines rolling window size

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
    (1,101,1,100),
    (1,101,2,200),
    (1,101,3,300),
    (1,101,4,400),
    (1,101,5,500)
]

df = spark.createDataFrame(data, ["emp_id","item_no","week","salary"])

df.show()
# Step 2 — Define Windows
window_2week = Window.partitionBy("emp_id","item_no") \
    .orderBy("week") \
    .rowsBetween(-1,0)

window_4week = Window.partitionBy("emp_id","item_no") \
    .orderBy("week") \
    .rowsBetween(-3,0)

# rowsBetween(-1,0) → last 2 weeks
# rowsBetween(-3,0) → last 4 weeks

# Step 3 — Calculate Rolling Salaries

result = df.withColumn(
    "salary_last_2_weeks",
    sum("salary").over(window_2week)
).withColumn(
    "salary_last_4_weeks",
    sum("salary").over(window_4week)
)

result.show()

#
# Simple Flow
# Partition → emp_id, item_no
# ↓
# Order → week
# ↓
# Window size → 2 weeks / 4 weeks
# ↓
# SUM(salary)

