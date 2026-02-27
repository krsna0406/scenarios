"""
input:
join records
"""
# print(__doc__)

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



from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinTest").getOrCreate()

data1 = [(1,), (1,), (None,), (None,)]
data2 = [(1,), (None,)]

df1 = spark.createDataFrame(data1, ["id"])
df2 = spark.createDataFrame(data2, ["id"])

df1.show()
df2.show()


df1.join(df2, "id", "inner").show()
df1.join(df2, "id", "left").show()
df1.join(df2, "id", "right").show()
df1.join(df2, "id", "outer").show()

