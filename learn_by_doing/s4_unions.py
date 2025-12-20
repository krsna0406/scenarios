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

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
data1 = [("James","Sales","NY",90000,34,10000), \
         ("Michael","Sales","NY",86000,56,20000), \
         ("Robert","Sales","CA",81000,30,23000), \
         ("Maria","Finance","CA",90000,24,23000) \
         ]

columns= ["employee_name","department","state","salary","age","bonus"]

df =spark.createDataFrame(data=data1,schema=columns)
df.show()

data2 = [("James","Sales","NY",90000,34,10000), \
         ("Maria","Finance","CA",90000,24,23000), \
         ("Jen","Finance","NY",79000,53,15000), \
         ("Jeff","Marketing","CA",80000,25,18000), \
         ("Kumar","Marketing","NY",91000,50,21000) \
         ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 =spark.createDataFrame(data=data2,schema=columns2)
df2.show()

from pyspark.sql import Window

window_spec=Window.partitionBy().orderBy("salary")

# union
print("...........union")
df.union(df2).withColumn("row", row_number().over(window_spec)).show()

#df1.except(df2)

# union by removing dups
print("...........union by removing the dups")
df.exceptAll(df2).union(df2).withColumn("row", row_number().over(window_spec)).show()



# union in sql
print("...........union in spark SQL")
df.createOrReplaceTempView("sqldf")
df2.createOrReplaceTempView("sqldf1")
sqldf=spark.sql("select * from sqldf union  select * from sqldf1")
sqldf.withColumn("row", row_number().over(window_spec)).show()

#nionall
print("...........unionall")
df.unionAll(df2).withColumn("row", row_number().over(window_spec)).show()


#df diff 1. Except / Subtract – rows in df1 but NOT in df2
print("...........diff of dfs")
left_diff  = df.exceptAll(df2)

left_diff.show()
# rows in df1 but NOT in df2
right_diff  = df2.exceptAll(df)

right_diff.show()

diff = left_diff.unionAll(right_diff)

diff.show()


ubynamedf=df.unionByName(df2,allowMissingColumns=True)
ubynamedf.show()