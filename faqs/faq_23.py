"""
. Write a sql query to find out top 2 highest price products in each category.

input:

+----------+------------+-----------+-----+
|product_id|product_name|category   |price|
+----------+------------+-----------+-----+
|1         |Phone       |Electronics|500  |
|2         |Laptop      |Electronics|1200 |
|3         |TV          |Electronics|900  |
|4         |Shirt       |Clothing   |40   |
|5         |Jacket      |Clothing   |120  |
|6         |Jeans       |Clothing   |80   |
+----------+------------+-----------+-----+

output:
+----------+------------+-----------+-----+
|product_id|product_name|category   |price|
+----------+------------+-----------+-----+
|2         |Laptop      |Electronics|1200 |
|3         |TV          |Electronics|900  |
|5         |Jacket      |Clothing   |120  |
|6         |Jeans       |Clothing   |80   |
+----------+------------+-----------+-----+

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
from pyspark.sql.functions import col, count, when

# Initialize Spark
spark = SparkSession.builder.appName("DropColumnsWithNulls").getOrCreate()

data = [
    (1,"Phone","Electronics",500),
    (2,"Laptop","Electronics",1200),
    (3,"TV","Electronics",900),
    (4,"Shirt","Clothing",40),
    (5,"Jacket","Clothing",120),
    (6,"Jeans","Clothing",80)
]

columns = ["product_id","product_name","category","price"]

df = spark.createDataFrame(data,columns)

df.show()
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

windowSpec = Window.partitionBy("category").orderBy(col("price").desc())

result = df.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") <= 2) \
    .drop("rank")

result.show()