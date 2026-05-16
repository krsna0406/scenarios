"""

SQL query for getting  employee salaries above avg salary in a department  SQL

1️⃣ Using Correlated Subquery (Most common)

SELECT emp_id, name, dept_id, salary
FROM employee e
WHERE salary >
      (SELECT AVG(salary)
       FROM employee
       WHERE dept_id = e.dept_id);

Using JOIN (More efficient in large datasets)

SELECT e.emp_id, e.name, e.dept_id, e.salary
FROM employee e
JOIN (
      SELECT dept_id, AVG(salary) avg_sal
      FROM employee
      GROUP BY dept_id
     ) d
ON e.dept_id = d.dept_id
WHERE e.salary > d.avg_sal;




3) by using CTE

WITH cte AS (
    SELECT deptid, AVG(salary) AS avg_sal
    FROM employee
    GROUP BY deptid
)
SELECT e.*
FROM employee e
JOIN cte
    ON e.deptid = cte.deptid
WHERE e.salary > cte.avg_sal;




imp note:


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

