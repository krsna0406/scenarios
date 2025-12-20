"""

scenario 30: Write a SQL Query to extract second most salary for each department
input:
+------+----+-------+-------+
|emp_id|name|dept_id| salary|
+------+----+-------+-------+
|     1|   A|      A|1000000|
|     2|   B|      A|2500000|
|     3|   C|      G| 500000|
|     4|   D|      G| 800000|
|     5|   E|      W|9000000|
|     6|   F|      W|2000000|
+------+----+-------+-------+

+--------+---------+
|dept_id1|dept_name|
+--------+---------+
|       A|    AZURE|
|       G|      GCP|
|       W|      AWS|
+--------+---------+

expected :

+------+----+---------+-------+
|emp_id|name|dept_name| salary|
+------+----+---------+-------+
|     1|   A|    AZURE|1000000|
|     6|   F|      AWS|2000000|
|     3|   C|      GCP| 500000|
+------+----+---------+-------+

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


data1 = [
    (1, "A", "A", 1000000),
    (2, "B", "A", 2500000),
    (3, "C", "G", 500000),
    (4, "D", "G", 800000),
    (5, "E", "W", 9000000),
    (6, "F", "W", 2000000),
]
df1 = spark.createDataFrame(data1, ["emp_id", "name", "dept_id", "salary"])
df1.show()

data2 = [("A", "AZURE"), ("G", "GCP"), ("W", "AWS")]
df2 = spark.createDataFrame(data2, ["dept_id1", "dept_name"])
df2.show()

#
# print("DSL")
# joindf=df1.join(df2,df1.dept_id==df2.dept_id1 ,"inner").drop("dept_id1")
# joindf.show()
#
# from pyspark.sql import Window
#
# window=Window.partitionBy("dept_id").orderBy(col("salary").desc())
#
# joindf.withColumn("rank",dense_rank().over(window))\
#     .filter(col("rank")==2)\
#     .select("emp_id","name","dept_name","salary")\
#     .show(truncate=False)

print("SPARK SQL")

df1.createOrReplaceTempView("sqldf1")
df2.createOrReplaceTempView("sqldf2")

spark.sql(""" select * from (
select a.*,b.* ,dense_rank() over( partition by dept_id order by salary desc) as rank from sqldf1 a 
inner join sqldf2 b on a.dept_id=b.dept_id1 ) where rank=2
""").select("emp_id","name","dept_name","salary").show(truncate=False)