"""

Two Dataframes - customers & segment
In segment we have a column contains: Premium, Gold, Silver, Bronze
Need to fetch customers with the highest level segment tier,
 if they have more than one segment.

input:

+-----------+-------------+
|customer_id|customer_name|
+-----------+-------------+
|1          |John         |
|2          |Sara         |
|3          |Mike         |
|4          |David        |
+-----------+-------------+

+-----------+-------+
|customer_id|segment|
+-----------+-------+
|1          |Silver |
|1          |Gold   |
|2          |Bronze |
|2          |Silver |
|3          |Premium|
|3          |Gold   |
|4          |Bronze |
+-----------+-------+


output:

+-----------+-------------+-------+
|customer_id|customer_name|segment|
+-----------+-------------+-------+
|1          |John         |Gold   |
|2          |Sara         |Silver |
|3          |Mike         |Premium|
|4          |David        |Bronze |
+-----------+-------------+-------+






note:

SQL:

SQL Version (Often Asked)
SELECT customer_id, segment
FROM (
    SELECT *,
           ROW_NUMBER() OVER(
           PARTITION BY customer_id
           ORDER BY
           CASE segment
                WHEN 'Premium' THEN 4
                WHEN 'Gold' THEN 3
                WHEN 'Silver' THEN 2
                WHEN 'Bronze' THEN 1
           END DESC
           ) rn
    FROM segment
) t
WHERE rn = 1;



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
from pyspark.sql.functions import count, sum, avg, round
spark = SparkSession.builder.appName("EmployeeBonus").getOrCreate()

# 1. Create Customers DataFrame

customers_data = [
    (1, "John"),
    (2, "Sara"),
    (3, "Mike"),
    (4, "David")
]
customers_cols = ["customer_id", "customer_name"]
customers = spark.createDataFrame(customers_data, customers_cols)
customers.show()

# 2. Create Segment DataFrame
segment_data = [
    (1, "Silver"),
    (1, "Gold"),
    (2, "Bronze"),
    (2, "Silver"),
    (3, "Premium"),
    (3, "Gold"),
    (4, "Bronze")
]

segment_cols = ["customer_id", "segment"]
segment = spark.createDataFrame(segment_data, segment_cols)
segment.show()

# 3. Segment Priority

# Priority:
#
# Premium > Gold > Silver > Bronze

# Step 1 — Assign ranking to segments
from pyspark.sql.functions import when, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
segment_ranked = segment.withColumn(
    "rank",
    when(col("segment")=="Premium",4)
    .when(col("segment")=="Gold",3)
    .when(col("segment")=="Silver",2)
    .when(col("segment")=="Bronze",1)
)

print('segment_ranked')
segment_ranked.show()
# Step 2 — Get highest segment per customer
w = Window.partitionBy("customer_id").orderBy(col("rank").desc())

highest_segment = segment_ranked.withColumn(
    "rn",
    row_number().over(w)
).filter(col("rn")==1)
# Step 3 — Join with customers
result = customers.join(
        highest_segment,
        "customer_id",
        "left"
    ).select("customer_id","customer_name","segment")

result.show()