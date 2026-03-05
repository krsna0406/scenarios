"""
Show the count of employees per salary

input:
+------+----+------+
|emp_id|name|salary|
+------+----+------+
|1     |A   |5000  |
|2     |B   |7000  |
|3     |C   |5000  |
|4     |D   |8000  |
|5     |E   |7000  |
+------+----+------+
imp note:
SQL:

SELECT salary, COUNT(*) AS emp_count
FROM employee
GROUP BY salary
ORDER BY salary;


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
from pyspark.sql.functions import col, count, when, expr

# Initialize Spark
spark = SparkSession.builder.appName("DropColumnsWithNulls").getOrCreate()
data = [
    (1,"A",5000),
    (2,"B",7000),
    (3,"C",5000),
    (4,"D",8000),
    (5,"E",7000)
]

columns = ["emp_id","name","salary"]

df = spark.createDataFrame(data,columns)

df.show()

df.groupBy("salary").count().show()