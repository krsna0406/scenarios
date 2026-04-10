"""
to compute the average processing time for each machine.

input:

+----------+----------+-------------+---------+
|machine_id|process_id|activity_type|timestamp|
+----------+----------+-------------+---------+
|         0|         0|        start|    0.712|
|         0|         0|          end|     1.52|
|         0|         1|        start|     3.14|
|         0|         1|          end|     4.12|
|         1|         0|        start|     0.55|
|         1|         0|          end|     1.55|
|         1|         1|        start|     0.43|
|         1|         1|          end|     1.42|
|         2|         0|        start|      4.1|
|         2|         0|          end|    4.512|
|         2|         1|        start|      2.5|
|         2|         1|          end|      5.0|
+----------+----------+-------------+---------+


note:


Each process has:

a start timestamp
an end timestamp

Processing time for a process:

processing_time = end_time - start_time
Then:
avg_processing_time = average(processing_time) per machine

SQL Solution ::

SELECT
    machine_id,
    ROUND(AVG(end_time - start_time),3) AS avg_processing_time
FROM
(
    SELECT
        machine_id,
        process_id,
        MAX(CASE WHEN activity_type='end' THEN timestamp END) AS end_time,
        MAX(CASE WHEN activity_type='start' THEN timestamp END) AS start_time
    FROM Activity
    GROUP BY machine_id, process_id
) t
GROUP BY machine_id;


logic:

Interview Trick

This problem tests 3 key skills:

Self aggregation

CASE WHEN pivoting

Group by twice

Pattern:

start / end rows → convert to columns → subtract → average


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
    (0, 0, "start", 0.712),
    (0, 0, "end", 1.520),
    (0, 1, "start", 3.140),
    (0, 1, "end", 4.120),
    (1, 0, "start", 0.550),
    (1, 0, "end", 1.550),
    (1, 1, "start", 0.430),
    (1, 1, "end", 1.420),
    (2, 0, "start", 4.100),
    (2, 0, "end", 4.512),
    (2, 1, "start", 2.500),
    (2, 1, "end", 5.000)
]

columns = ["machine_id", "process_id", "activity_type", "timestamp"]

df = spark.createDataFrame(data, columns)

df.show()

from pyspark.sql.functions import col, when, max, avg, round

df2 = df.groupBy("machine_id","process_id").agg(
    max(when(col("activity_type")=="start",col("timestamp"))).alias("start_time"),
    max(when(col("activity_type")=="end",col("timestamp"))).alias("end_time")
)

result = df2.withColumn(
    "processing_time",
    col("end_time") - col("start_time")
)

final = result.groupBy("machine_id").agg(
    round(avg("processing_time"),3).alias("avg_processing_time")
)

final.show()

