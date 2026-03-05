"""
5.	T_CUST_DETAIL
CUST_ID, PROD_ID, Purchase_Date
Find customers who have made a purchase every month for the last six months.


input:




output:


imp note:

trunc(current_date(), "month")

Let’s break it down precisely.

1️⃣ current_date()

From:
pyspark.sql.functions

It returns:

The current system date (no time component).

Example:

If today is 2026-03-15

current_date()

Result:

2026-03-15
2️⃣ trunc(date, "month")

trunc() = truncate date to a specified unit

Syntax:

trunc(date_column, format)

Supported formats:

"year"

"yyyy"

"month"

"mm"

When you use:

trunc(date_col, "month")

It returns:

The first day of that month

3️⃣ Combined Expression
trunc(current_date(), "month")

If today = 2026-03-15

Step-by-step:

current_date() → 2026-03-15

trunc(..., "month") → 2026-03-01

So the result is:

2026-03-01
4️⃣ Why We Use It in the 6-Month Query

When calculating last 6 calendar months:

add_months(trunc(current_date(), "month"), -5)

If today = March 15, 2026

trunc(current_date(), "month")
→ 2026-03-01

add_months(..., -5)
→ 2025-10-01

So the window becomes:

2025-10-01 to 2026-03-31

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
spark = SparkSession.builder.appName("ex").getOrCreate()


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType
from pyspark.sql.functions import col
from datetime import date

spark = SparkSession.builder.appName("CustomerData").getOrCreate()

# Define schema
schema = StructType([
    StructField("CUST_ID", IntegerType(), True),
    StructField("PROD_ID", IntegerType(), True),
    StructField("PURCHASE_DATE", DateType(), True)
])

# Sample data
data = [
    # ✅ Customer 101 (Qualifies)
    (101, 1, date(2025, 10, 5)),
    (101, 2, date(2025, 11, 10)),
    (101, 3, date(2025, 12, 15)),
    (101, 1, date(2026, 1, 20)),
    (101, 2, date(2026, 2, 12)),
    (101, 3, date(2026, 3, 3)),

    # ❌ Customer 102 (Missing December)
    (102, 1, date(2025, 10, 2)),
    (102, 2, date(2025, 11, 11)),
    (102, 1, date(2026, 1, 9)),
    (102, 3, date(2026, 2, 18)),
    (102, 1, date(2026, 3, 22)),

    # ❌ Customer 103 (Only 4 months)
    (103, 1, date(2025, 12, 1)),
    (103, 2, date(2026, 1, 5)),
    (103, 3, date(2026, 2, 7)),
    (103, 1, date(2026, 3, 14)),

    # ✅ Customer 104 (Multiple purchases per month, still qualifies)
    (104, 1, date(2025, 10, 3)),
    (104, 2, date(2025, 10, 25)),
    (104, 1, date(2025, 11, 6)),
    (104, 2, date(2025, 12, 8)),
    (104, 3, date(2026, 1, 10)),
    (104, 1, date(2026, 2, 15)),
    (104, 2, date(2026, 3, 19)),
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

df.show()

df_last6 = df.filter(
    col("Purchase_Date") >= add_months(trunc(current_date(), "month"), -5)
)

df_month = df_last6.withColumn(
    "purchase_month",
    trunc(col("Purchase_Date"), "month")
)
df_month.show()
result = (
    df_month.groupBy("CUST_ID")
    .agg(countDistinct("purchase_month").alias("month_count"))
    .filter(col("month_count") == 6)
)

result.show()


