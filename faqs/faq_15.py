"""


input:

data = [
    ("Hello world",),
    ("Hello Spark",),
    ("Hello Python Spark",),
    ("Big data with Spark and Python",),
    ("World of data and analytics",)
]



output:

Hello: 3
World: 2
Spark: 3
Python: 2
Big: 1
Data: 2
With: 1
And: 1
Analytics: 1


imp note:
 with RDD

 rdd1=rdd.flatMap(lambda x: x[0].lower().split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
# rdd1.collect()
# rdd1.foreach(lambda  x: print(x))
for x in rdd1.collect():
    print(x)




"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql import  Window
spark = SparkSession.builder.appName("ex").getOrCreate()
data=[
    ("Hello world",),
    ("Hello Spark",),
    ("Hello Python Spark",),
    ("Big data with Spark and Python",),
    ("World of data and analytics",)
]
df=spark.createDataFrame(data,["text"])
df.show(truncate=False)


dfex=df.withColumn("text",expr ("""  explode(split(text,' '))"""))
dfex.show()


cntdf=dfex.groupby("text").agg(count("*")).alias("cnt")
cntdf.show()

print(cntdf.collect())

for row in cntdf.collect():
    print(row[0]," ",row[1])


