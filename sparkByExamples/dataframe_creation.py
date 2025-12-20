"""
Here are all common ways to create a DataFrame in PySpark, including RDD, iterable data, Row, namedtuple, struct types, and schema-based creations.

✅ 1. From a Python List / Tuple (Most Common)
Without schema (Spark infers automatically)
data = [(1, "Alice"), (2, "Bob")]
df = spark.createDataFrame(data)
df.show()

With schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df = spark.createDataFrame(data, schema)

✅ 2. From a List of Dictionaries

Automatically converts keys to columns.

data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
]
df = spark.createDataFrame(data)
df.show()

✅ 3. From RDD
rdd = spark.sparkContext.parallelize([(1, "Alice"), (2, "Bob")])
df = spark.createDataFrame(rdd, ["id", "name"])

✅ 4. From RDD + Row Object
from pyspark.sql import Row

rdd = spark.sparkContext.parallelize([
    Row(id=1, name="Alice"),
    Row(id=2, name="Bob")
])

df = spark.createDataFrame(rdd)

✅ 5. From NamedTuple

Useful when you want typed records.

from collections import namedtuple

Person = namedtuple("Person", ["id", "name"])

data = [Person(1, "Alice"), Person(2, "Bob")]

df = spark.createDataFrame(data)
df.show()

✅ 6. Using Spark createDataFrame + Column Names
data = [(1, "Alice"), (2, "Bob")]
columns = ["id", "name"]

df = spark.createDataFrame(data, columns)

✅ 7. Using toDF() on RDD or List
From list of tuples:
df = spark.sparkContext.parallelize([(1,"Alice"), (2,"Bob")]).toDF(["id","name"])

✅ 8. Using JSON (string or file)
From JSON string:
json_data = ['{"id":1,"name":"Alice"}', '{"id":2,"name":"Bob"}']
df = spark.read.json(spark.sparkContext.parallelize(json_data))

✅ 9. Using Pandas DataFrame
import pandas as pd

pdf = pd.DataFrame({"id": [1,2], "name": ["Alice", "Bob"]})
df = spark.createDataFrame(pdf)

✅ 10. Using Row + StructType Schema
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

data = [Row(id=1, name="Alice"), Row(id=2, name="Bob")]
df = spark.createDataFrame(data, schema)

✅ 11. From a Single Column (zip example)

👉 Your example:
zip() is used because Spark expects iterable rows.
We convert list → list of single-element tuples.

dates = ["2019-07-01 12:01:19.111", "2019-06-24 12:01:19.222"]

df = spark.createDataFrame(list(zip(dates)), ["dates"])
df.show()

✅ 12. Using Spark SQL (Temporary View)
spark.sql("""'SELECT 1 AS id, Alice AS name'""")

✅ 13.Create a DataFrame with column names specified.

spark. createDataFrame([('Alice', 1)], "name: string, age: int").show()
+—–+—+
| name|age|
+—–+—+
|Alice|  1|
+—–+—+

✅ 14.Create an empty DataFrame. When initializing an empty DataFrame in PySpark,
 it's mandatory to specify its schema, as the DataFrame lacks data from which the schema can be inferred.
>>> spark. createDataFrame([], "name: string, age: int").show()
+—-+—+
|name|age|
+—-+—+
+—-+—+

"""

print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, Row

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

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

rdd = spark.sparkContext.parallelize(data)

print("rdd.toDF()")
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()

dfFromData2 = spark.createDataFrame(data).toDF(*columns)
dfFromData2.printSchema()

rowData = map(lambda x: Row(*x), data)
dfFromData3 = spark.createDataFrame(rowData,columns)
dfFromData3.printSchema()