"""

scenario 14 : We need total marks
input:
+------+------+------+-------+-----+-------+------+
|rollno|  name|telugu|english|maths|science|social|
+------+------+------+-------+-----+-------+------+
|203040|rajesh|    10|     20|   30|     40|    50|
+------+------+------+-------+-----+-------+------+
expected :

+------+------+------+-------+-----+-------+------+-----+
|rollno|  name|telugu|english|maths|science|social|total|
+------+------+------+-------+-----+-------+------+-----+
|203040|rajesh|    10|     20|   30|     40|    50|  150|
+------+------+------+-------+-----+-------+------+-----+


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

data = [
        (203040, "rajesh", 10, 20, 30, 40, 50)
]

df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
df.show()

df.withColumn("social",col("telugu")+col("english")+col("maths")+col("science")+col("social")).show()


# Through SQL
df.createOrReplaceTempView("marks")
spark.sql("select *, (telugu+english+maths+science+social) as total from marks").show()

# Through DSL
finaldf = df.withColumn("total", expr("telugu+english+maths+science+social")).show()