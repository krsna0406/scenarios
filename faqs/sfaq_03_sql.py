"""

I have 2 table
call table with country id receiver id and also log details
2nd table with country code Country name now i wanted to know which is domestic call
and which is international call so if country id and reciver id is from same country
sthen its domestic
and if not then international and
also i need to know the percentage of international call


Assume two tables.

Table 1: call_logs
call_id	caller_country_id	receiver_country_id

Table 2: country
| country_id | country_name |

Goal:

Identify Domestic vs International calls

Calculate percentage of international calls


1️⃣ Classify Each Call
SELECT
    call_id,
    CASE
        WHEN caller_country_id = receiver_country_id
        THEN 'Domestic'
        ELSE 'International'
    END AS call_type
FROM call_logs;


Logic

caller_country_id = receiver_country_id → Domestic
caller_country_id ≠ receiver_country_id → International

2️⃣ Percentage of International Calls
SELECT
    ROUND(
        SUM(CASE WHEN caller_country_id <> receiver_country_id THEN 1 ELSE 0 END) * 100.0
        / COUNT(*),
        2
    ) AS international_percentage
FROM call_logs;

Explanation

SUM(CASE WHEN caller_country_id <> receiver_country_id THEN 1 END)
        → counts international calls

COUNT(*) → total calls

(international / total) * 100
3️⃣ With Country Names (Join)
SELECT
    cl.call_id,
    c2.country_name AS caller_country,
    c2.country_name AS receiver_country,
    CASE
        WHEN cl.caller_country_id = cl.receiver_country_id
        THEN 'Domestic'
        ELSE 'International'
    END AS call_type
FROM call_logs cl
JOIN country c1
    ON cl.caller_country_id = c1.country_id
JOIN country c2
    ON cl.receiver_country_id = c2.country_id;
Example Output
call_id	caller_country	receiver_country	call_type
1	India	India	Domestic
2	India	USA	International
3	UK	UK	Domestic

✔ Interview Tip:
This question tests:

CASE WHEN

JOIN

Conditional aggregation (SUM(CASE...))


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
from pyspark.sql.functions import *


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()

call_data = [
    (1,1,1),
    (2,1,2),
    (3,2,2),
    (4,3,1)
]

country_data = [
    (1,"India"),
    (2,"USA"),
    (3,"UK")
]

call_df = spark.createDataFrame(call_data,
                                ["call_id","caller_country_id","receiver_country_id"])

country_df = spark.createDataFrame(country_data,
                                   ["country_id","country_name"])

call_df.show()
country_df.show()


#Step 2 — Identify Domestic / International Calls
call_type_df = call_df.withColumn(
    "call_type",
    when(col("caller_country_id") == col("receiver_country_id"),
         "Domestic").otherwise("International")
)

call_type_df.show()

# Step 3 — Percentage of International Calls

result = call_type_df.select(
    round(
        (sum(when(col("call_type")=="International",1)
             .otherwise(0))*100)/count("*"),
        2
    ).alias("international_percentage")
)

result.show()

# Step 4 — Add Country Names (Join)

caller = country_df.withColumnRenamed("country_id","caller_country_id") \
    .withColumnRenamed("country_name","caller_country")

receiver = country_df.withColumnRenamed("country_id","receiver_country_id") \
    .withColumnRenamed("country_name","receiver_country")

final_df = call_df.join(caller,"caller_country_id") \
    .join(receiver,"receiver_country_id")

final_df.show()

#
# Simple Flow
# Call Logs
# ↓
# Compare caller_country_id & receiver_country_id
# ↓
# Domestic / International
# ↓
# Aggregate
# ↓
# International Percentage
