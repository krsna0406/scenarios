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


🔴 Step 1: What .collect() returns
df.select(min("date").alias("min_date"),
          max("date").alias("max_date")).collect()
Output:
[Row(min_date=datetime.date(2025, 9, 1),
     max_date=datetime.date(2025, 9, 3))]
🧠 Meaning
.collect() returns a list
Each element = a Row object
Here:
List has 1 row
That row has 2 fields
🔴 Step 2: Access first row
date_range = df.select(...).collect()[0]
Now:
date_range = Row(min_date=2025-09-01, max_date=2025-09-03)

👉 So:

date_range is NOT a list anymore
It is a Row object (like a dict + object hybrid)
🔴 Step 3: Access values
✔️ Method 1 (recommended)
date_range["max_date"]

👉 Output:

datetime.date(2025, 9, 3)
✔️ Method 2 (dot notation)
date_range.max_date

👉 Same result

🧠 Internals (important)

Row behaves like:

✔️ Dictionary → row["col"]
✔️ Object → row.col
🔍 Full flow summary
result = df.select(min("date"), max("date")).collect()
Step-by-step:
result = [Row(min_date=..., max_date=...)]   # list

result[0]                                    # Row

result[0]["max_date"]                        # actual value





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
df.printSchema()
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

print("collect()")
print(df.select(
    min("order_date").alias("min_date"),
    max("order_date").alias("max_date")
).collect())

print("collect()[0]")
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



