"""

scenario 22 :(Cumilative sum)
input:

+---+------+-----+
|pid|  date|price|
+---+------+-----+
|  1|26-May|  100|
|  1|27-May|  200|
|  1|28-May|  300|
|  2|29-May|  400|
|  3|30-May|  500|
|  3|31-May|  600|
+---+------+-----+
expected :
+---+------+-----+---------+
|pid|  date|price|new_price|
+---+------+-----+---------+
|  1|26-May|  100|      100|
|  1|27-May|  200|      300|
|  1|28-May|  300|      600|
|  2|29-May|  400|      400|
|  3|30-May|  500|      500|
|  3|31-May|  600|     1100|
+---+------+-----+---------+

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
data = [(1, "26-May", 100),
        (1, "27-May", 200),
        (1, "28-May", 300),
        (2, "29-May", 400),
        (3, "30-May", 500),
        (3, "31-May", 600)]
df = spark.createDataFrame(data, ["pid", "date", "price"])
df.show()

from pyspark.sql import  Window

window=Window.partitionBy("pid").orderBy("price")
print("DSL")

df.withColumn("new_price",sum("price").over(window)).show()


print("SPARK SQL")

df.createOrReplaceTempView("sqldf")

spark.sql("""
select *,sum(price) over( partition by pid order by date ) new_price from sqldf
""").show(truncate=False)