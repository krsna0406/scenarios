"""
Get the highest 3 fuel_consumed data

input:

+---------+-------------+
|device_id|fuel_consumed|
+---------+-------------+
|       v1|          100|
|       v2|          140|
|       v1|          303|
|       v2|          943|
|       v3|          309|
|       v1|          492|
|       v1|          842|
|       v2|          493|
+---------+-------------+

imp note:

SELECT device_id, fuel_consumed
FROM device_fuel
ORDER BY fuel_consumed DESC
LIMIT 3;


SELECT device_id, fuel_consumed
FROM (
    SELECT device_id,
           fuel_consumed,
           ROW_NUMBER() OVER(ORDER BY fuel_consumed DESC) AS rn
    FROM device_fuel
) t
WHERE rn <= 3;







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
data = [("Eng",),("Aus",),("Ind",)]

data = [
    ("v1",100),
    ("v2",140),
    ("v1",303),
    ("v2",943),
    ("v3",309),
    ("v1",492),
    ("v1",842),
    ("v2",493)
]

df = spark.createDataFrame(data,["device_id","fuel_consumed"])

df.show()

# Get top 3

df.orderBy(df.fuel_consumed.desc()).limit(3).show()

# 4️⃣ PySpark using Window Function

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

w = Window.orderBy(col("fuel_consumed").desc())

df.withColumn("rn", row_number().over(w)) \
    .filter(col("rn") <= 3) \
    .show()