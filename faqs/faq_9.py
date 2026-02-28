"""

join 2 tables and remove duplicates

input:

+-----+----+--------------------+---------+-------+------+
|EmpID|Name|         Designation| HireDate|Dept_No|Salary|
+-----+----+--------------------+---------+-------+------+
|    1| Sam|          Consultant|17-Feb-20|     15| 30000|
|    2| Ron|     System Engineer|24-Apr-22|     12| 20000|
|    3| Anu|  Technology Analyst|12-Jan-20|     21| 35000|
|    4|Arun|     System Engineer|07-Jul-24|     16| 25000|
|    5| Anu|Senior System Eng...|12-Jan-20|     21| 26300|
|    5| Anu|  Technology Analyst|12-Jan-20|     21| 35000|
+-----+----+--------------------+---------+-------+------+

+-------+------+
|Dept_No|Rating|
+-------+------+
|     15|   3.0|
|     12|   5.0|
|     16|   4.0|
|     21|   4.0|
|     21|   3.5|
+-------+------+


imp note:

✅ Step 1: Remove duplicates properly

We must decide business logic:
For Employee table → keep latest salary? highest salary? distinct row?
For Dept table → keep highest rating? average rating? distinct?
Since not specified, I will:
Remove exact duplicate employee rows
Take maximum rating per department


SQL IMP ***

SELECT DISTINCT e.*, d.rating
FROM Emp e
JOIN (
    SELECT Dept_No, MAX(Rating) AS Rating
    FROM Dept
    GROUP BY Dept_No
) d
ON e.Dept_No = d.Dept_No;


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

spark = SparkSession.builder.appName("example").getOrCreate()

emp_data = [
    (1, "Sam", "Consultant", "17-Feb-20", 15, 30000),
    (2, "Ron", "System Engineer", "24-Apr-22", 12, 20000),
    (3, "Anu", "Technology Analyst", "12-Jan-20", 21, 35000),
    (4, "Arun", "System Engineer", "07-Jul-24", 16, 25000),
    (5, "Anu", "Senior System Engineer", "12-Jan-20", 21, 26300),
    (3, "Anu", "Technology Analyst", "12-Jan-20", 21, 35000)
]

emp_columns = ["EmpID", "Name", "Designation", "HireDate", "Dept_No", "Salary"]

emp_df = spark.createDataFrame(emp_data, emp_columns)

emp_df.show()


dept_data = [
    (15, 3.0),
    (12, 5.0),
    (16, 4.0),
    (21, 4.0),
    (21, 3.5)
]

# # dept_columns = ["Dept_No", "Rating"]
#
# dept_df = spark.createDataFrame(dept_data, dept_columns)
#pyspark.errors.exceptions.base.PySparkTypeError: [CANNOT_MERGE_TYPE]
# Can not merge type `LongType` and `DoubleType`.
# so imposed schema
#refer https://chatgpt.com/c/699efb53-6030-8323-87bc-ead5cb9005e6



dept_schema = StructType([
    StructField("Dept_No", IntegerType(), True),
    StructField("Rating", DoubleType(), True)
])

dept_df = spark.createDataFrame(dept_data, schema=dept_schema)

dept_df.show()


# 1️⃣ Remove exact duplicates from employee table
emp_clean = emp_df.dropDuplicates()

emp_clean.show()

# 2️⃣ Remove duplicate dept rows (take max rating per dept)
dept_clean = dept_df.groupBy("Dept_No") \
    .agg(max("Rating").alias("Rating"))

dept_clean.show()

result1 = emp_clean.join(
    dept_clean,
    emp_clean["Dept_No"] == dept_clean["Dept_No"],
    "inner"
)
# 3️⃣ Join
result = emp_clean.join(
    dept_clean,
    emp_clean["Dept_No"] == dept_clean["Dept_No"],
    "inner"
).select(
    emp_clean["*"],
    dept_clean["Rating"]
)

result.show()

from pyspark.sql.functions import count

emp_df.select(count("HireDate").alias("hiredate_count")).show()



result1.show()