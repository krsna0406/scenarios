"""
flattening the data
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

from pyspark.sql.functions import  *

spark = SparkSession.builder.appName("JoinTest").getOrCreate()

data = [
    (1, "Alice", [{"id": 101, "price": 50}, {"id": 102, "price": 70}]),
    (2, "Bob", [{"id": 103, "price": 200}])
]

columns = ["order_id", "customer", "items"]

df = spark.createDataFrame(data, columns)

df.show(truncate=False)
df.printSchema()

df2=df.withColumn("items",expr("explode(items)"))
df2.show(truncate=False)
df2.printSchema()

df2.selectExpr(
"order_id",
"customer","items.id","items.price"
).show()