"""

input:

+------+-------+----------+------+
|emp_id|   name|department|salary|
+------+-------+----------+------+
|     1|  Alice|        HR|  5000|
|     2|    Bob|   Finance|  6000|
|     3|Charlie|        HR|  7000|
|     4|  David|   Finance|  8000|
|     5|    Eve|        IT|  9000|
|     5|    Eve|        IT| 10000|
+------+-------+----------+------+

output:

+------+-------+-

---------+------+---------------+
|emp_id|   name|department|salary|dept_avg_salary|
+------+-------+----------+------+---------------+
|     4|  David|   Finance|  8000|         7000.0|
|     3|Charlie|        HR|  7000|         6000.0|
|     5|    Eve|        IT| 10000|         9500.0|
+------+-------+----------+------+---------------+

note:

SQL Solution

SQL Solution 1 — Using Subquery (Most Common)

SELECT *
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM employees
    WHERE department = e.department
);

Logic

For each employee:

salary > avg(department_salary)
SQL Solution 2 — Using Window Function (Better for Interviews)
SELECT *
FROM (
    SELECT *,
           AVG(salary) OVER(PARTITION BY department) AS dept_avg
    FROM employees
) t
WHERE salary > dept_avg;
SQL Solution 3 — Using GROUP BY + JOIN
SELECT e.*
FROM employees e
JOIN (
    SELECT department, AVG(salary) AS dept_avg
    FROM employees
    GROUP BY department
) d
ON e.department = d.department
WHERE e.salary > d.dept_avg;




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
    (1, "Alice", "HR", 5000),
    (2, "Bob", "Finance", 6000),
    (3, "Charlie", "HR", 7000),
    (4, "David", "Finance", 8000),
    (5, "Eve", "IT", 9000),
    (5, "Eve", "IT", 10000)
]

columns = ["emp_id", "name", "department", "salary"]

employees = spark.createDataFrame(data, columns)
employees.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col

w = Window.partitionBy("department")

result = employees.withColumn(
    "dept_avg_salary",
    avg("salary").over(w)
).filter(
    col("salary") > col("dept_avg_salary")
)

result.show()


