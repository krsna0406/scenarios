"""

scenario 24 :
input:
+------+------------+
|userid|        page|
+------+------------+
|     1|        home|
|     1|    products|
|     1|    checkout|
|     1|confirmation|
|     2|        home|
|     2|    products|
|     2|        cart|
|     2|    checkout|
|     2|confirmation|
|     2|        home|
|     2|    products|
+------+------------+


expected :

+------+--------------------------------------------------------------+
|userid|pages                                                         |
+------+--------------------------------------------------------------+
|1     |[home, products, checkout, confirmation]                      |
|2     |[home, products, cart, checkout, confirmation, home, products]|
+------+--------------------------------------------------------------+


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
from pyspark.sql.functions import repeat, col

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()



data = [
    (1, "home"),
    (1, "products"),
    (1, "checkout"),
    (1, "confirmation"),
    (2, "home"),
    (2, "products"),
    (2, "cart"),
    (2, "checkout"),
    (2, "confirmation"),
    (2, "home"),
    (2, "products")]
df = spark.createDataFrame(data, ["userid", "page"])
df.show()

print("DSL")

df.groupby("userid").agg(collect_list(col("page")).alias("pages")).show(truncate=False)

print("SPARK SQL")

df.createOrReplaceTempView("sqldf")
spark.sql("""
select userid,collect_list(page) pages from sqldf group by userid order by userid
""").show(truncate=False)