"""

scenario2: Need the dates when the status gets changed like ordered to dispatched

input:

+-------+----------+----------+
|orderid|statusdate|    status|
+-------+----------+----------+
|      1|     1-Jan|   Ordered|
|      1|     2-Jan|dispatched|
|      1|     3-Jan|dispatched|
|      1|     4-Jan|   Shipped|
|      1|     5-Jan|   Shipped|
|      1|     6-Jan| Delivered|
|      2|     1-Jan|   Ordered|
|      2|     2-Jan|dispatched|
|      2|     3-Jan|   shipped|
+-------+----------+----------+
expected

+-------+----------+----------+
|orderid|statusdate|    status|
+-------+----------+----------+
|      1|     2-Jan|dispatched|
|      1|     3-Jan|dispatched|
|      2|     2-Jan|dispatched|
+-------+----------+----------+


"""

print(__doc__)

import  time
print(time.time())

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
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "dispatched"),
    (1, "3-Jan", "dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "dispatched"),
    (2, "3-Jan", "shipped")]
myschema = ["orderid","statusdate","status"]
df = spark.createDataFrame(data,schema=myschema)
df.show()


# get the order id which is having status as dispatched and aswell in the order status list.

#SPARK SQL

#
# df.createOrReplaceTempView("sqldf")
#
# spark.sql("""
# select * from sqldf where status ='dispatched' and orderid in (
# select orderid from sqldf where status='Ordered'
# )
# """).show()

# arrStatus=df.filter("status='Ordered'").collect()

# print("arrStatus    ",arrStatus)


arr= [row[0]  for row in df.filter( col("status") == "Ordered" ).collect()]

# for row in arrStatus:
#     print(" row items  []: ",row[0])
#     print(" row items  . : ",row.orderid)

#DSL
df.printSchema()
print("SPARK DSL")

df.filter( (col("status") == "dispatched") & (col("orderid").isin(*arr)) ).show()


#get the

# df.filter(
#     (col("status") == "dispatched") &
#     (col("orderid").isin(
#         *[row[0] for row in df.filter(col("status") == "Ordered").select("orderid").collect()]
#     ))
# )

