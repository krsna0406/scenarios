# """
# convert column python list
#
# 🔍 Step-by-step explanation
#
# Assume:
#
# states1 = ["KA", "TN", "KA", "MH", "TN"]
#
# 1️⃣ OrderedDict.fromkeys(states1) produces:
# OrderedDict([
#     ("KA", None),
#     ("TN", None),
#     ("MH", None)
# ])
#
# 2️⃣ Convert to list:
# res = ["KA", "TN", "MH"]
#
#
# Duplicates removed, order preserved.
#
# | Method                          | Removes duplicates | Preserves order |
# | ------------------------------- | ------------------ | --------------- |
# | `set()`                         | ✔️                 | ❌               |
# | `OrderedDict.fromkeys()`        | ✔️                 | ✔️              |
# | `dict.fromkeys()` (Python ≥3.7) | ✔️                 | ✔️              |
#
# """

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

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()
data = [("James","Smith","USA","CA"),("Michael","Rose","USA","NY"), \
        ("Robert","Williams","USA","CA"),("Maria","Jones","USA","FL") \
        ]
columns=["firstname","lastname","country","state"]
df=spark.createDataFrame(data=data,schema=columns)
df.show()
print(df.collect())

df1=df.select(df["state"])

print(df1.rdd.collect())

print(df1.rdd.map(lambda x: x[0]).collect())

print(set(df1.rdd.map(lambda x: x[0]).collect()))

print(list(set(df1.rdd.map(lambda x: x[0]).collect())))

