"""

scenario 17 : df1 contains Employeeid,Name,Age,State,Country columns
              df2 contains Employeeid,Name,Age,Address columns.
                how do you merge df1 and df2
                to get the following output Employeeid,Name,Age,State,Country,Address
input:
+------+-----+---+------+-------+
|emp_id| name|age| state|country|
+------+-----+---+------+-------+
|     1|  Tim| 24|Kerala|  India|
|     2|Asman| 26|Kerala|  India|
+------+-----+---+------+-------+

+------+-----+---+-------+
|emp_id| name|age|address|
+------+-----+---+-------+
|     1|  Tim| 24|Comcity|
|     2|Asman| 26|bimcity|
+------+-----+---+-------+



expected :

+------+-----+---+------+-------+-------+
|emp_id| name|age| state|country|address|
+------+-----+---+------+-------+-------+
|     1|  Tim| 24|Kerala|  India|Comcity|
|     2|Asman| 26|Kerala|  India|bimcity|
+------+-----+---+------+-------+-------+


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

data = [(1, "Tim", 24, "Kerala", "India"),
        (2, "Asman", 26, "Kerala", "India")]
df1 = spark.createDataFrame(data, ["emp_id", "name", "age", "state", "country"])
df1.show()

data2 = [(1, "Tim", 24, "Comcity"),
         (2, "Asman", 26, "bimcity")]
df2 = spark.createDataFrame(data2, ["emp_id", "name", "age", "address"])
df2.show()

# df1.unionByName(df2,allowMissingColumns=True).show()

df1.join(df2,["emp_id", "name", "age"],"outer").show()

df1.join(df2,["emp_id", "name", "age"],"inner").show()

df1.join(df2,["emp_id", "name", "age"],"left").show()


