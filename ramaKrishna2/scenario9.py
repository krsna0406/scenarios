"""

scenario 9:(write spark code, list of name of participants who has rank=1 most number of times)

input:
+----+---------------+
|name|           rank|
+----+---------------+
|   a|   [1, 1, 1, 3]|
|   b|   [1, 2, 3, 4]|
|   c|[1, 1, 1, 1, 4]|
|   d|            [3]|
+----+---------------+

expected : c

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
        ("a", [1, 1, 1, 3]),
        ("b", [1, 2, 3, 4]),
        ("c", [1, 1, 1, 1, 4]),
        ("d", [3])
]
df = spark.createDataFrame(data, ["name", "rank"])
df.show()
#
# explodedf = df.withColumn("rank", explode(col("rank")))
# explodedf.show()
#
# filtdf = explodedf.filter(col("rank") == 1)
# filtdf.show()
#
# countdf = filtdf.groupBy("name").agg(count("*").alias("count")).orderBy(col("count").desc())
# countdf.show()
#
# finaldf = countdf.select(col("name")).first()[0]
# print(finaldf)
#
#




# revision


df.printSchema()

df1=df.withColumn("rank", expr("explode(rank)")).groupBy("name")\
        .agg(count("rank").alias('rank')).orderBy(col("rank").desc() )

df1.show()

print()

print(df1.select("name").collect()[0]["name"])


print("SPARK SQL")

df.createOrReplaceTempView("sqldf")


spark.sql(""" 

with cte as (
select name, explode(rank) as rank  from sqldf
), cte2 as (
select name,count(rank) as count from cte group by name order by count desc limit 1)

select name from cte2
""").show()