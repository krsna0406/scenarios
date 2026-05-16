"""
How do you calculate the running sum of sales for each product category?
input:

+-----------+----------+-----+
|   category|      date|sales|
+-----------+----------+-----+
|Electronics|2023-10-01|  100|
|Electronics|2023-10-02|  200|
|   Clothing|2023-10-01|  150|
|   Clothing|2023-10-02|  100|
|Electronics|2023-10-03|  300|
+-----------+----------+-----+


output:

+-----------+----------+-----+-------------+
|   category|      date|sales|running_sales|
+-----------+----------+-----+-------------+
|   Clothing|2023-10-01|  150|          150|
|   Clothing|2023-10-02|  100|          250|
|Electronics|2023-10-01|  100|          100|
|Electronics|2023-10-02|  200|          300|
|Electronics|2023-10-03|  300|          600|
+-----------+----------+-----+-------------+


note:

SQL Solution

SELECT
    category,
    date,
    sales,
    SUM(sales) OVER(
        PARTITION BY category
        ORDER BY date
    ) AS running_sales
FROM sales_table;

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
    ("Electronics", "2023-10-01", 100),
    ("Electronics", "2023-10-02", 200),
    ("Clothing", "2023-10-01", 150),
    ("Clothing", "2023-10-02", 100),
    ("Electronics", "2023-10-03", 300),
]

columns = ["category", "date", "sales"]

from pyspark.sql.window import Window
from pyspark.sql.functions import sum, col

df = spark.createDataFrame(data, columns)

df.show()

w = Window.partitionBy("category").orderBy("date")

result = df.withColumn(
    "running_sales",
    sum("sales").over(w)
)

result.show()


