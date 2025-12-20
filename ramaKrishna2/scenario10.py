"""

scenario 10 :
input:

+-----+-------------+-------------+
|empid|commissionamt|monthlastdate|
+-----+-------------+-------------+
|    1|          300|  31-Jan-2021|
|    1|          400|  28-Feb-2021|
|    1|          200|  31-Mar-2021|
|    2|         1000|  31-Oct-2021|
|    2|          900|  31-Dec-2021|
+-----+-------------+-------------+

expected :

+-----+-------------+-------------+
|empid|commissionamt|monthlastdate|
+-----+-------------+-------------+
|    1|          200|  31-Mar-2021|
|    2|         1000|  31-Oct-2021|
+-----+-------------+-------------+
"""

print(__doc__)

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

from pyspark import SparkContext,SparkConf
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()

data = [
    (1, 300, "31-Jan-2021"),
    (1, 400, "28-Feb-2021"),
    (1, 200, "31-Mar-2021"),
    (2, 1000, "31-Oct-2021"),
    (2, 900, "31-Dec-2021")
]
df = spark.createDataFrame(data, ["empid", "commissionamt", "monthlastdate"])
df.show()

maxdatedf = df.groupBy(col("empid").alias("empid1")).agg(max("monthlastdate").alias("maxdate"))
maxdatedf.show()

joindf = df.join(maxdatedf, (df["empid"] == maxdatedf["empid1"]) & (df["monthlastdate"] == maxdatedf["maxdate"]),
                 "inner").drop("empid1", "maxdate")
joindf.show()
