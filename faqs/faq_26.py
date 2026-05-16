"""
source = [
    {"order_id": 1, "customer_id": 1, "amount": 10000},
    {"order_id": 2, "customer_id": 2, "amount": 300},
    {"order_id": 3, "customer_id": 3, "amount": 4000},
    {"order_id": 4, "customer_id": 1, "amount": 4000},
    {"order_id": 5, "customer_id": 2, "amount": 8000},
    {"order_id": 6, "customer_id": 4, "amount": 100},
]
    op = [
    {"cusomer_id": 1, "order_ids": [1, 4], "total": 14000},
    {"customer_id": 4, "order_ids": [6], "total": 100},
]


input:

+------+-----------+--------+
|amount|customer_id|order_id|
+------+-----------+--------+
| 10000|          1|       1|
|   300|          2|       2|
|  4000|          3|       3|
|  4000|          1|       4|
|  8000|          2|       5|
|   100|          4|       6|
+------+-----------+--------+

output:

+-----------+---------+-----+
|customer_id|order_ids|total|
+-----------+---------+-----+
|1          |[1, 4]   |14000|
+-----------+---------+-----+


imp note:
 please note on importing the sum from functions

SQL:

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
from pyspark.sql.functions import col, count, when, expr, collect_list,sum

# Initialize Spark
spark = SparkSession.builder.appName("DropColumnsWithNulls").getOrCreate()

source = [
    {"order_id": 1, "customer_id": 1, "amount": 10000},
    {"order_id": 2, "customer_id": 2, "amount": 300},
    {"order_id": 3, "customer_id": 3, "amount": 4000},
    {"order_id": 4, "customer_id": 1, "amount": 4000},
    {"order_id": 5, "customer_id": 2, "amount": 8000},
    {"order_id": 6, "customer_id": 4, "amount": 100},
]

df=spark.createDataFrame(source)
df.show()


result = (
    df.groupBy("customer_id")
    .agg(
        collect_list("order_id").alias("order_ids"),
        sum("amount").alias("total")
    )
    .filter(col("total") > 10000)
)

result.show(truncate=False)