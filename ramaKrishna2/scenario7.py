"""

scenario 7:

INPUT:

+-------+----------+----+--------+-----+
|sale_id|product_id|year|quantity|price|
+-------+----------+----+--------+-----+
|      1|       100|2010|      25| 5000|
|      2|       100|2011|      16| 5000|
|      3|       100|2012|       8| 5000|
|      4|       200|2010|      10| 9000|
|      5|       200|2011|      15| 9000|
|      6|       200|2012|      20| 7000|
|      7|       300|2010|      20| 7000|
|      8|       300|2011|      18| 7000|
|      9|       300|2012|      20| 7000|
+-------+----------+----+--------+-----+


OUTPUT:


+-------+----------+----+--------+-----+
|sale_id|product_id|year|quantity|price|
+-------+----------+----+--------+-----+
|      6|       200|2012|      20| 7000|
|      9|       300|2012|      20| 7000|
|      1|       100|2010|      25| 5000|
|      8|       300|2011|      18| 7000|
+-------+----------+----+--------+-----+
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
    (1, 100, 2010, 25, 5000),
    (2, 100, 2011, 16, 5000),
    (3, 100, 2012, 8, 5000),
    (4, 200, 2010, 10, 9000),
    (5, 200, 2011, 15, 9000),
    (6, 200, 2012, 20, 7000),
    (7, 300, 2010, 20, 7000),
    (8, 300, 2011, 18, 7000),
    (9, 300, 2012, 20, 7000)
]
myschema = ["sale_id", "product_id", "year", "quantity", "price"]
df = spark.createDataFrame(data, schema=myschema)
df.show()


from pyspark.sql import  Window

# ** IMP DK by  using rowsbetween

# window=Window.partitionBy("product_id").orderBy("quantity").rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
#
# df.withColumn("max_quantity",max("quantity").over(window))\
#     .filter(col("quantity")==col("max_quantity")).drop("max_quantity").show()

print("DSL")
window=Window.partitionBy("product_id").orderBy(col("quantity").desc())

df.withColumn("rank",dense_rank().over(window))\
    .filter(col("rank")==1).show()

print("SPARK SQL")

df.createOrReplaceTempView("sqldf")
spark.sql("""select * from(
select *, dense_rank() over ( partition by product_id order by quantity desc ) as rank from sqldf) t1
where rank=1 
""").show()