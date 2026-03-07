"""
input:

+-----+----------+-----------+
|Sl_No|First_Name|Change_Date|
+-----+----------+-----------+
|    1|       Ram| 01-01-1998|
|    2|       Raj| 01-01-1998|
|    3|      Hari| 01-01-1998|
|    1|       Ram| 01-01-2026|
+-----+----------+-----------+

output:

Sl No	Fist Name	        Change Date
2	       Raj         	    01-01-1998
3	       Hari 	        01-01-1998
1	       Ram 	            01-01-2026



imp note:

SQL:

# SQL query

SELECT Sl_No, First_Name, Change_Date
FROM (
    SELECT *,
    ROW_NUMBER() OVER (
    PARTITION BY Sl_No
ORDER BY TO_DATE(Change_Date,'DD-MM-YYYY') DESC
) AS rn
FROM employee_changes
) t
WHERE rn = 1
ORDER BY Sl_No





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
    (1, "Ram", "01-01-1998"),
    (2, "Raj", "01-01-1998"),
    (3, "Hari", "01-01-1998"),
    (1, "Ram", "01-01-2026")
]

df = spark.createDataFrame(data, ["Sl_No", "First_Name", "Change_Date"])
df.show()

df.printSchema()

# Convert Change_Date to date type
df = df.withColumn("Change_Date", to_date(col("Change_Date"), "dd-MM-yyyy"))

df.printSchema()
# Define window: partition by employee, order by date descending
window_spec = Window.partitionBy("Sl_No").orderBy(col("Change_Date").desc())

# Apply row_number to get the latest record per Sl_No
df_latest = df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .orderBy("Sl_No")

df_latest.show()
