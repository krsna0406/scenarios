"""
SQL - Below is the Teams table & required the below Output   SQL

Coln
Eng
Aus
Ind

Output

Coln_1 |Coln_2
Aus    |Ind
Aus    |Eng
Eng    |Ind


imp note:
 SQL:
 SELECT
    t1.Coln AS Coln_1,
    t2.Coln AS Coln_2
FROM Teams t1
JOIN Teams t2
ON t1.Coln < t2.Coln;

You can also write using ROW_NUMBER
WITH cte AS (
SELECT Coln,
       ROW_NUMBER() OVER(ORDER BY Coln) rn
FROM Teams
)

SELECT
t1.Coln,
t2.Coln
FROM cte t1
JOIN cte t2
ON t1.rn < t2.rn;



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
df = spark.createDataFrame(data, ["Coln"])

df.show()

# df1=df.alias("t1")
# df2=df.alias("t2")
#
# df1.join(df2,(col("t1.Coln")<col("t2.Coln")),"inner").show()


result = df.alias("t1") \
    .join(df.alias("t2"), col("t1.Coln") < col("t2.Coln")) \
    .select(
    col("t1.Coln").alias("Coln_1"),
    col("t2.Coln").alias("Coln_2")
)

result.show()