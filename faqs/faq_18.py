"""
Find hire date and termination date from this table in sql
Op should empid(without duplicating) hire date and termination date

input:
+------+-----------+----------+
|emp_id| event_type|event_date|
+------+-----------+----------+
|   101|       HIRE|2020-01-10|
|   101|TERMINATION|2024-03-15|
|   102|       HIRE|2021-06-01|
|   103|       HIRE|2019-02-20|
|   103|TERMINATION|2022-11-30|
+------+-----------+----------+





output:
emp_id	hire_date	termination_date
101	2020-01-10	2024-03-15
102	2021-06-01	NULL
103	2019-02-20	2022-11-30


imp note:

SQL:
Recommended SQL Solution (Conditional Aggregation)
SELECT
    emp_id,
    MAX(CASE WHEN event_type = 'HIRE' THEN event_date END)        AS hire_date,
    MAX(CASE WHEN event_type = 'TERMINATION' THEN event_date END) AS termination_date
FROM employee_events
GROUP BY emp_id;



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

data = [
    (101, "HIRE", "2020-01-10"),
    (101, "TERMINATION", "2024-03-15"),
    (102, "HIRE", "2021-06-01"),
    (103, "HIRE", "2019-02-20"),
    (103, "TERMINATION", "2022-11-30")
]

columns = ["emp_id", "event_type", "event_date"]

df = spark.createDataFrame(data, columns) \
    .withColumn("event_date", col("event_date").cast("date"))

df.show()

result_df = (
    df.groupBy("emp_id")
    .agg(
        min(when(col("event_type") == "HIRE", col("event_date")))
        .alias("hire_date"),
        min(when(col("event_type") == "TERMINATION", col("event_date")))
        .alias("termination_date")
    )
)
result_df.show()