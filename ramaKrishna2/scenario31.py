"""

scenario 31:
input:
+----+-----+--------+-----------+
|col1| col2|    col3|       col4|
+----+-----+--------+-----------+
|  m1|m1,m2|m1,m2,m3|m1,m2,m3,m4|
+----+-----+--------+-----------+

expected :
+-----------+
|        col|
+-----------+
|         m1|
|      m1,m2|
|   m1,m2,m3|
|m1,m2,m3,m4|
|           |
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
# creating the dataframe

data = [("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")]

df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
df.show()

df.withColumn("col", expr("""

concat(col1,'-',col2,'-',col3,'-',col4)

""")).withColumn("col", explode(split(col("col"),"-")))\
    .drop("col1","col2","col3","col4").show(truncate=False)