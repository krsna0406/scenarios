"""

input:

pyspark scenario based question 1  SCENARIO
input:
+---+------+-----------------+
| id|  name|           skills|
+---+------+-----------------+
|101|  siva|python,C#,pyspark|
|102|dinesh|        AWS,Azure|
|103|  Kavi|           Python|
+---+------+-----------------+

Output:

+---+------+-------+
| id|  name| skills|
+---+------+-------+
|101|  siva| python|
|101|  siva|     C#|
|101|  siva|pyspark|
|102|dinesh|    AWS|
|102|dinesh|  Azure|
|103|  Kavi| Python|
+---+------+-------+


note:
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
    (101, "siva", "python,C#,pyspark"),
    (102, "dinesh", "AWS,Azure"),
    (103, "Kavi", "Python")
]

df = spark.createDataFrame(data, ["id","name","skills"])

df.show()
result = df.withColumn("skills", explode(split("skills", ",")))

result.show()