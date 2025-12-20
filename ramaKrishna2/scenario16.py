"""

scenario 16 : Remove duplicates
input:
+---+----+-----------+------+
| id|name|       dept|salary|
+---+----+-----------+------+
|  1|Jhon|    Testing|  5000|
|  2| Tim|Development|  6000|
|  3|Jhon|Development|  5000|
|  4| Sky| Prodcution|  8000|
+---+----+-----------+------+

expected :
+---+----+-----------+------+
| id|name|       dept|salary|
+---+----+-----------+------+
|  1|Jhon|    Testing|  5000|
|  2| Tim|Development|  6000|
|  4| Sky| Prodcution|  8000|
+---+----+-----------+------+
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


data = [(1, "Jhon", "Testing", 5000),
        (2, "Tim", "Development", 6000),
        (3, "Jhon", "Development", 5000),
        (4, "Sky", "Prodcution", 8000)]
df = spark.createDataFrame(data, ["id", "name", "dept", "salary"])
df.show()


df.drop_duplicates(["name","salary"]).show()