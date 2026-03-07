"""

input:
+----+-------+------+------+
|name|  city1| city2| city3|
+----+-------+------+------+
|siva|chennai|  pune| delhi|
|hari|     NA|Trichy|    NA|
|mani|       | Delhi|Trichy|
+----+-------+------+------+

output:

+----+-------+------+------+---------+
|name|  city1| city2| city3|finalcity|
+----+-------+------+------+---------+
|siva|chennai|  pune| delhi|  chennai|
|hari|     NA|Trichy|    NA|   Trichy|
|mani|       | Delhi|Trichy|    Delhi|
+----+-------+------+------+---------+

note:

SQL Solution

SELECT
    name,
    city1,
    city2,
    city3,
    CASE
        WHEN city1 IS NOT NULL AND city1 <> 'NA' AND city1 <> '' THEN city1
        WHEN city2 IS NOT NULL AND city2 <> 'NA' AND city2 <> '' THEN city2
        WHEN city3 IS NOT NULL AND city3 <> 'NA' AND city3 <> '' THEN city3
    END AS finalcity
FROM table_name;


clean version:


SELECT *,
COALESCE(
    NULLIF(city1,'NA'),
    NULLIF(city2,'NA'),
    NULLIF(city3,'NA')
) AS finalcity
FROM table_name;






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
    ("siva","chennai","pune","delhi"),
    ("hari","NA","Trichy","NA"),
    ("mani","","Delhi","Trichy")
]

df = spark.createDataFrame(data, ["name","city1","city2","city3"])

df.show()

result = df.withColumn(
    "finalcity",
    when((col("city1")!="NA") & (col("city1")!=""), col("city1"))
    .when((col("city2")!="NA") & (col("city2")!=""), col("city2"))
    .when((col("city3")!="NA") & (col("city3")!=""), col("city3"))
)

result.show()


# Using coalesce + nullif style logic:
# coalesce() picks first non-null value.
from pyspark.sql.functions import coalesce, when, col

df2 = df.select(
    "*",
    coalesce(
        when((col("city1")!="NA") & (col("city1")!=""), col("city1")),
        when((col("city2")!="NA") & (col("city2")!=""), col("city2")),
        when((col("city3")!="NA") & (col("city3")!=""), col("city3"))
    ).alias("finalcity")
)

df2.show()