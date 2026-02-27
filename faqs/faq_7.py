"""

Write a pyspark code to find the customer who purchases daily.

input:

| customer_id | order_id | order_date |
| ----------- | -------- | ---------- |
| 1           | 101      | 2025-09-01 |
| 1           | 102      | 2025-09-02 |
| 1           | 103      | 2025-09-03 |
| 2           | 104      | 2025-09-01 |
| 2           | 105      | 2025-09-03 |

Customer 1 purchased daily (no missing date).
Customer 2 missed 2025-09-02.


output:

+------------+
|customer_id|
+------------+
|           1|
+------------+


imp note:


Logic Explained Simply

Find total number of days in dataset.

Count how many distinct days each customer purchased.

If purchase_days == total_days → customer purchased daily.


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
    (1, 101, "2025-09-01"),
    (1, 102, "2025-09-02"),
    (1, 103, "2025-09-03"),
    (2, 104, "2025-09-01"),
    (2, 105, "2025-09-03")
]

df = spark.createDataFrame(data, ["customer_id", "order_id", "order_date"])

df = df.withColumn("order_date", col("order_date").cast("date"))

df.show()

df.printSchema()

# Get overall date range
df.select(
    min("order_date").alias("min_date"),
    max("order_date").alias("max_date")
).show()
date_range = df.select(
    min("order_date").alias("min_date"),
    max("order_date").alias("max_date")
).collect()[0]


print("date_range---",date_range)

# date_range1 = df.select(
#     min("order_date").alias("min_date"),
#     max("order_date").alias("max_date")
# ).collect()
#
# print("check---",date_range1)


total_days = (date_range["max_date"] - date_range["min_date"]).days + 1

# Count distinct purchase days per customer
daily_customers = df.groupBy("customer_id") \
    .agg(countDistinct("order_date").alias("purchase_days")) \
    .filter(col("purchase_days") == total_days)

daily_customers.show()



