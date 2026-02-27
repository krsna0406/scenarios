"""

Write a pyspark code to find the avg salary of all department and
find the department which is getting higher than the avg salary

We will solve this in two clear steps:

1️⃣ Find average salary per department
2️⃣ Find departments whose average salary is greater than overall company average salary


input:

+------+-------+------+
|emp_id|   dept|salary|
+------+-------+------+
|   101|     IT| 10000|
|   102|     IT| 15000|
|   103|     HR|  8000|
|   104|     HR|  9000|
|   105|Finance| 20000|
|   106|Finance| 22000|
+------+-------+------+



imp note:


"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



from pyspark.sql import SparkSession

from pyspark.sql.functions import  *

spark = SparkSession.builder.appName("StudentData").getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql import  Window

spark = SparkSession.builder.appName("EmployeePerformance").getOrCreate()
# Sample Data
data = [
    (101, "IT", 10000),
    (102, "IT", 15000),
    (103, "HR", 8000),
    (104, "HR", 9000),
    (105, "Finance", 20000),
    (106, "Finance", 22000)
]
df = spark.createDataFrame(data, ["emp_id", "dept", "salary"])
df.show()

# Step 1: Average salary per department
dept_avg = df.groupBy("dept") \
    .agg(avg("salary").alias("dept_avg_salary"))

print("average salary of each department")
dept_avg.show()

# Step 2: Overall average salary
overall_avg = df.agg(avg("salary").alias("overall_avg")).collect()[0][0]

print("overall_avg-=======  ",overall_avg)
# Step 3: Departments earning higher than overall average
result = dept_avg.filter(col("dept_avg_salary") > overall_avg)

result.show()



