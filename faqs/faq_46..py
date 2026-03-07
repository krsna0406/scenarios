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

scenario 1- Find the second-highest salary for each department.
scenario 2 - Find the cumulative sum of salary for each department.



note:

SQL Solution


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
from pyspark.sql.functions import dense_rank, col

w = Window.partitionBy("department").orderBy(col("salary").desc())

result = employees.withColumn(
    "rank", dense_rank().over(w)
).filter(col("rank") == 2)

result.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

w = Window.partitionBy("department").orderBy(col("salary").desc())

result = employees.withColumn(
    "rank", dense_rank().over(w)
).filter(col("rank") == 2)

result.show()


from pyspark.sql.functions import sum

w = Window.partitionBy("department").orderBy("salary")

result = employees.withColumn(
    "cumulative_salary",
    sum("salary").over(w)
)

result.show()