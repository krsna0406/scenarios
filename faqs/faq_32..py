"""
write a program using pyspark using 2 table(1.find the highest salary and highest in every department)

assume tables like below:

+------+----+-------+------+
|emp_id|name|dept_id|salary|
+------+----+-------+------+
|     1|   A|    101|  5000|
|     2|   B|    101|  7000|
|     3|   C|    102|  6000|
|     4|   D|    102|  8000|
|     5|   E|    103|  4000|
+------+----+-------+------+

+-------+---------+
|dept_id|dept_name|
+-------+---------+
|    101|       IT|
|    102|       HR|
|    103|      FIN|
+-------+---------+




imp note:


SQL:

Assume two tables

Employee
emp_id	name	dept_id	salary
Department

| dept_id | dept_name |

Goal:
Find the employee with the highest salary in each department.

SQL Query
SELECT e.emp_id,
       e.name,
       d.dept_name,
       e.salary
FROM employee e
JOIN department d
    ON e.dept_id = d.dept_id
JOIN (
        SELECT dept_id,
               MAX(salary) AS max_salary
        FROM employee
        GROUP BY dept_id
     ) m
ON e.dept_id = m.dept_id
AND e.salary = m.max_salary;
Example Data

employee

emp_id	name	dept_id	salary
1	A	101	5000
2	B	101	7000
3	C	102	6000
4	D	102	8000
5	E	103	4000

department

dept_id	dept_name
101	IT
102	HR
103	FIN
Output
emp_id	name	dept_name	salary
2	B	IT	7000
4	D	HR	8000
5	E	FIN	4000
Interview Preferred SQL (Window Function)
SELECT emp_id,
       name,
       dept_id,
       salary
FROM (
        SELECT e.*,
               ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn
        FROM employee e
     ) t
WHERE rn = 1;




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


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
# Step 1 — Create DataFrames
emp_data = [
    (1,"A",101,5000),
    (2,"B",101,7000),
    (3,"C",102,6000),
    (4,"D",102,8000),
    (5,"E",103,4000)
]

dept_data = [
    (101,"IT"),
    (102,"HR"),
    (103,"FIN")
]

emp_df = spark.createDataFrame(emp_data,["emp_id","name","dept_id","salary"])
dept_df = spark.createDataFrame(dept_data,["dept_id","dept_name"])

emp_df.show()
dept_df.show()

# Step 2 — Find highest salary in each department
max_sal_df = emp_df.withColumn("salary", col("salary").cast("int")).groupBy("dept_id") \
    .agg(max("salary").alias("max_salary"))


# Step 3 — Join with employee table
emp_max = emp_df.join(
    max_sal_df,
    (emp_df.dept_id == max_sal_df.dept_id) &
    (emp_df.salary == max_sal_df.max_salary)
)
# Step 4 — Join with department table
final_df = emp_max.join(dept_df, emp_max.dept_id == dept_df.dept_id) \
    .select("emp_id","name","dept_name","salary")

final_df.show()

# using window function

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("dept_id").orderBy(col("salary").desc())

df = emp_df.withColumn("rank", row_number().over(windowSpec))

df.filter(col("rank")==1).show()


#check the errors later

