"""

scenario 28 :
input:
+-----+------+
|child|parent|
+-----+------+
|    A|    AA|
|    B|    BB|
|    C|    CC|
|   AA|   AAA|
|   BB|   BBB|
|   CC|   CCC|
+-----+------+

expected :
+-----+------+-----------+
|child|parent|grandparent|
+-----+------+-----------+
|    A|    AA|        AAA|
|    C|    CC|        CCC|
|    B|    BB|        BBB|
+-----+------+-----------+

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

data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

df = spark.createDataFrame(data, ["child", "parent"])
df.show()

joindf = df.alias("a").join(df.alias("b"), col("a.child") == col("b.parent")).select(
    col("a.child").alias("child_a"),
    col("a.parent").alias("parent_a"),
    col("b.child").alias("child_b"),
    col("b.parent").alias("parent_b")
)
joindf.show()

findf = joindf.withColumnRenamed("child_a", "parent").withColumnRenamed("parent_a", "grandparent").withColumnRenamed(
    "child_b", "child").drop("parent_b").select("child", "parent", "grandparent")

findf.show()

# another way

df2 = df.withColumnRenamed("child", "child1").withColumnRenamed("parent", "parent1")
df2.show()

secondjoindf = df.join(df2, col("parent") == col("child1"), "inner")
secondjoindf.show()

finaldf = secondjoindf.withColumnRenamed("parent1", "grandparent").drop("child1")
finaldf.show()