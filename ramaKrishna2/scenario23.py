"""

scenario 23 :

Goal: find customers who purchased all products present in product table.

input:

+-----------+-----------+
|customer_id|product_key|
+-----------+-----------+
|          1|          5|
|          2|          6|
|          3|          5|
|          3|          6|
|          1|          6|
+-----------+-----------+

+-----------+
|product_key|
+-----------+
|          5|
|          6|
+-----------+

expected :

+-----------+
|customer_id|
+-----------+
|          1|
|          3|
+-----------+
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

data = [(1, 5), (2, 6), (3, 5), (3, 6), (1, 6)]
df = spark.createDataFrame(data, ["customer_id", "product_key"])
df.show()
data2 = [(5,), (6,)]
df2 = spark.createDataFrame(data2, ["product_key"])
df2.show()


print("SPARK SQL")

df.createOrReplaceTempView("custdf")
df2.createOrReplaceTempView("proddf")


spark.sql("""

with cte1 as(
select count(distinct product_key) as total_prod from proddf
), cte2 as (
select customer_id,count(product_key) count from custdf group by customer_id )

select customer_id from cte2 where count= (select total_prod from cte1)


""").show()


print("SPARK DSL")

total_products=df2.select("product_key").distinct().count()

print("total_products===",total_products)

df.groupBy("customer_id").agg(count("product_key").alias("count"))\
    .filter(col("count")==total_products).select("customer_id").show()







