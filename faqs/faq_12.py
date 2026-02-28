"""
you have customer data like IND,UK,SINGAPORE I need Singapore data first top of the DataFrame
second India third UK How to order it?

input:

+---------+-------------------+-----------+
|sensor_id|reading_time       |temperature|
+---------+-------------------+-----------+
|1        |2024-02-01 10:00:00|22.5       |
|1        |2024-02-01 10:01:00|22.7       |
|1        |2024-02-01 10:04:00|22.9       |
|1        |2024-02-01 10:05:00|23.0       |
|2        |2024-02-01 10:00:00|20.0       |
|2        |2024-02-01 10:03:00|20.3       |
+---------+-------------------+-----------+


output:

+---------+-------------------+-------------------+
|sensor_id|       missing_from|         missing_to|
+---------+-------------------+-------------------+
|        1|2024-02-01 10:02:00|2024-02-01 10:03:59|
|        2|2024-02-01 10:01:00|2024-02-01 10:02:59|
+---------+-------------------+-------------------+



imp note:

This is a time-series gap detection problem.

We must:

Partition by sensor_id
Order by reading_time
Compare current row with next row
If difference > 1 minute → gap exists

Report:

missing_from = previous_time + 1 minute
missing_to = next_time − 1 second

SQL:

✅ SQL Solution
WITH ordered AS (
    SELECT
        sensor_id,
        reading_time,
        LEAD(reading_time) OVER (
            PARTITION BY sensor_id
            ORDER BY reading_time
        ) AS next_time
    FROM sensor_readings
)

SELECT
    sensor_id,
    reading_time + INTERVAL '1' MINUTE AS missing_from,
    next_time - INTERVAL '1' SECOND AS missing_to
FROM ordered
WHERE next_time IS NOT NULL
  AND next_time > reading_time + INTERVAL '1' MINUTE;



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
    (1, "2024-02-01 10:00:00", 22.5),
    (1, "2024-02-01 10:01:00", 22.7),
    (1, "2024-02-01 10:04:00", 22.9),
    (1, "2024-02-01 10:05:00", 23.0),
    (2, "2024-02-01 10:00:00", 20.0),
    (2, "2024-02-01 10:03:00", 20.3)
]

columns = ["sensor_id", "reading_time", "temperature"]

df = spark.createDataFrame(data, columns)

# Convert reading_time to timestamp
df = df.withColumn("reading_time", to_timestamp("reading_time"))

df.show(truncate=False)
df.printSchema()

# DSL
window=Window.partitionBy("sensor_id").orderBy(col("reading_time"))
ldf=df.withColumn("next_time", lead("reading_time").over(window))

ldf.show()

fltrdf=ldf.filter((col("next_time").isNotNull()) & (col("next_time")> expr("reading_time+interval 1 minute")))


fltrdf.selectExpr("sensor_id","reading_time + interval 1 minute as missing_from","next_time - interval 1 second as missing_to").show()
#IMP POINT BELOW expr usage
# fltrdf.select(
#     "sensor_id",
#     expr("reading_time + interval 1 minute").alias("missing_from"),
#     expr("next_time - interval 1 second").alias("missing_to")
# ).show()