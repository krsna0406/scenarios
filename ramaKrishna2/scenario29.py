"""

scenario 29 :
input:

+---+
|col|
+---+
|  1|
|  2|
|  3|
+---+

+----+
|col1|
+----+
|   1|
|   2|
|   3|
|   4|
|   5|
+----+

expected :

+---+
|col|
+---+
|  1|
|  2|
|  4|
|  5|
+---+

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


data1 = [(1,), (2,), (3,)]

df1 = spark.createDataFrame(data1, ["col"])
df1.show()

data2 = [(1,), (2,), (3,), (4,), (5,)]

df2 = spark.createDataFrame(data2, ["col1"])
df2.show()


maxdf = df1.agg(max("col").alias("max"))
maxdf.show()


maxdf = df1.agg(max("col").alias("max"))
maxdf.show()

maxsalary = maxdf.select(col("max")).first()[0]
print("max val of df1--",maxsalary)

joindf = df1.join(df2, df1["col"] == df2["col1"], "outer").drop("col")
joindf.show()
finaldf = joindf.filter(col("col1") != maxsalary).withColumnRenamed("col1", "col").orderBy("col")
finaldf.show()