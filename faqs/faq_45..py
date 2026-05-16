"""
find the customers who placed order from alternative month?
input:

| order_id | customer_id | order_date |
| -------- | ----------- | ---------- |
| 1        | 101         | 2024-01-10 |
| 2        | 101         | 2024-03-15 |
| 3        | 101         | 2024-05-20 |
| 4        | 102         | 2024-01-12 |
| 5        | 102         | 2024-02-14 |

note:

SQL Solution

SQL Solution
Step 1 — Extract month and assign row number
SELECT
    customer_id,
    MONTH(order_date) AS month_num,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date) AS rn
FROM orders;
Step 2 — Check month difference = 2
SELECT customer_id
FROM (
    SELECT
        customer_id,
        MONTH(order_date) AS month_num,
        LAG(MONTH(order_date)) OVER(PARTITION BY customer_id ORDER BY order_date) prev_month
    FROM orders
) t
WHERE month_num - prev_month = 2;


Better Interview Solution
SELECT customer_id
FROM (
    SELECT customer_id,
           MONTH(order_date) m,
           LAG(MONTH(order_date)) OVER(PARTITION BY customer_id ORDER BY order_date) pm
    FROM orders
) t
GROUP BY customer_id
HAVING MIN(m - pm) = 2;




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
    (1,101,"2024-01-10"),
    (2,101,"2024-03-15"),
    (3,101,"2024-05-20"),
    (4,102,"2024-01-12"),
    (5,102,"2024-02-14")
]

df = spark.createDataFrame(data, ["order_id","customer_id","order_date"])

df.show()


# Step 2: Extract Month
df = df.withColumn("month_num", month("order_date"))
print("df with month")

df.show()
# Step 3: Use Window + LAG
w = Window.partitionBy("customer_id").orderBy("order_date")

df2 = df.withColumn("prev_month", lag("month_num").over(w))
# Step 4: Check Alternate Month Difference
print("df with previous month")

df2.show()

result = df2.withColumn("diff", col("month_num") - col("prev_month")) \
    .filter(col("diff") == 2) \
    .select("customer_id") \
    .distinct()

result.show()


# Shorter Interview Version
from pyspark.sql.functions import month, lag, col
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date")

df.withColumn("diff",
              month("order_date") - lag(month("order_date")).over(w)
              ).filter(col("diff")==2).select("customer_id").distinct()