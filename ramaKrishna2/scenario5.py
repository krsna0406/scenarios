"""

scenario5:

Read data from above file into dataframes(df1 and df2).
Display number of partitions in df1.
Create a new dataframe df3 from df1, along with a new column salary, and keep it constant 1000
append df2 and df3, and form df4
Remove records which have invalid email from df4, emails with @ are considered to be valid.
Write df4 to a target location, by partitioning on salary.
Input:

+---+----+---+-------------+
| id|name|age|        email|
+---+----+---+-------------+
|  1| abc| 31|abc@gmail.com|
|  2| def| 23| defyahoo.com|
|  3| xyz| 26|xyz@gmail.com|
|  4| qwe| 34| qwegmail.com|
|  5| iop| 24|iop@gmail.com|
+---+----+---+-------------+

+---+----+---+---------------+------+
| id|name|age|          email|salary|
+---+----+---+---------------+------+
| 11| jkl| 22|  abc@gmail.com|  1000|
| 12| vbn| 33|  vbn@yahoo.com|  3000|
| 13| wer| 27|            wer|  2000|
| 14| zxc| 30|        zxc.com|  2000|
| 15| lkj| 29|lkj@outlook.com|  2000|
+---+----+---+---------------+------+


output:

+---+----+---+---------------+------+
| id|name|age|          email|salary|
+---+----+---+---------------+------+
|  1| abc| 31|  abc@gmail.com|  1000|
|  3| xyz| 26|  xyz@gmail.com|  1000|
|  5| iop| 24|  iop@gmail.com|  1000|
| 11| jkl| 22|  abc@gmail.com|  1000|
| 12| vbn| 33|  vbn@yahoo.com|  3000|
| 15| lkj| 29|lkj@outlook.com|  2000|
+---+----+---+---------------+------+
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


# Read data convert into dataframes(df1 and df2).
data1 = [
        (1, "abc", 31, "abc@gmail.com"),
        (2, "def", 23, "defyahoo.com"),
        (3, "xyz", 26, "xyz@gmail.com"),
        (4, "qwe", 34, "qwegmail.com"),
        (5, "iop", 24, "iop@gmail.com"),
]
columns1=["id", "name", "age", "email"]
df1 = spark.createDataFrame(data1, columns1)

data2 = [
        (11, "jkl", 22, "abc@gmail.com", 1000),
        (12, "vbn", 33, "vbn@yahoo.com", 3000),
        (13, "wer", 27, "wer", 2000),
        (14, "zxc", 30, "zxc.com", 2000),
        (15, "lkj", 29, "lkj@outlook.com", 2000),
]
columns2=["id", "name", "age", "email", "salary"]
df2 = spark.createDataFrame(data2, columns2)

df1.show()
print("no of partitions   ",df1.rdd.getNumPartitions())
df2.show()
#
# print("   df3--------")
# df3=df1.withColumn("salary",lit(1000))
# df3.show()
#
# uniondf=df2.union(df3).orderBy("id")
# uniondf.show()
#
#
# df4=uniondf.filter(col("email").contains("@"))
#
# df4.show()
#
# # df4.write.partitionBy("salary").save("c://targetloc//")
#


print("SPARK SQL")

df1.createOrReplaceTempView("sqldf1")
df2.createOrReplaceTempView("sqldf2")

#*** imp
#
# spark.sql("""
# select * from (
# select * ,1000 as salary from sqldf1
# union
# select * from sqldf2) t1
# where email like '%@%'
# """).show()

# predicate pushdown

#*** imp

spark.sql("""
select * ,1000 as salary from sqldf1 where email like '%@%'
union
select * from sqldf2  where email like '%@%'
""").show()