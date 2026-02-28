"""
You're a Data Engineer at a company working on the UPI transaction analytics pipeline.
You receive daily transaction logs from multiple Indian banks. The data includes customer PAN,
bank name, transaction time, and amount. Due to fraud detection audits,
you need to find the Top 3 highest transactions per customer (based on PAN)
in the last 30 days.


input:
+----------+---------+-------------------+------+
|       PAN|bank_name|           txn_time|amount|
+----------+---------+-------------------+------+
|ABCDE1234F|      SBI|2024-06-01 10:00:00|  9000|
|ABCDE1234F|    ICICI|2024-06-15 11:30:00| 12000|
|ABCDE1234F|     HDFC|2024-06-20 09:45:00| 15000|
|ABCDE1234F|      SBI|2024-06-25 08:15:00| 11000|
|ABCDE1234F|      BOI|2024-06-27 14:05:00|  5000|
|ΧΥΖΑΒ99997|      SBI|2024-06-05 13:30:00|  7000|
|XYZAB9999Z|    ICICI|2024-06-12 15:30:00|  8000|
|ΧΥΖΑΒ99997|     HDFC|2024-06-20 17:30:00|  9500|
|ΧΥΖΑΒ99997|      SBI|2024-06-29 12:30:00|  6500|
+----------+---------+-------------------+------+



output:

imp note:

sql:
✅ SQL Solution

Assume table name: upi_transactions

Step 1: Filter last 30 days
Step 2: Rank transactions per PAN by amount
Step 3: Keep Top 3
WITH last_30_days AS (
    SELECT *
    FROM upi_transactions
    WHERE txn_time >= CURRENT_DATE - INTERVAL '30' DAY
),

ranked_txns AS (
    SELECT
        PAN,
        bank_name,
        txn_time,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY PAN
            ORDER BY amount DESC
        ) AS rn
    FROM last_30_days
)

SELECT *
FROM ranked_txns
WHERE rn <= 3;




"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql import  Window

spark = SparkSession.builder.appName("example").getOrCreate()
data = [
    ("ABCDE1234F", "SBI", "2024-06-01 10:00:00", 9000),
    ("ABCDE1234F", "ICICI", "2024-06-15 11:30:00", 12000),
    ("ABCDE1234F", "HDFC", "2024-06-20 09:45:00", 15000),
    ("ABCDE1234F", "SBI", "2024-06-25 08:15:00", 11000),
    ("ABCDE1234F", "BOI", "2024-06-27 14:05:00", 5000),
    ("ΧΥΖΑΒ99997", "SBI", "2024-06-05 13:30:00", 7000),
    ("XYZAB9999Z", "ICICI", "2024-06-12 15:30:00", 8000),
    ("ΧΥΖΑΒ99997", "HDFC", "2024-06-20 17:30:00", 9500),
    ("ΧΥΖΑΒ99997", "SBI", "2024-06-29 12:30:00", 6500)
]

columns = ["PAN", "bank_name", "txn_time", "amount"]

df = spark.createDataFrame(data, columns)
df.show()

df.printSchema()

from pyspark.sql.functions import to_timestamp
#Convert txn_time to timestamp
df = df.withColumn("txn_time", to_timestamp("txn_time"))

#Filter last 30 days

df_filtered = df.filter(
    col("txn_time") >= expr("current_timestamp() - interval 30 days")
)

df.show()

# Apply Window Function and filter top 3
window=Window.partitionBy(col("PAN")).orderBy(col("amount").desc())

df.withColumn("rank",row_number().over(window)).filter("rank<=3").show()