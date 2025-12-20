"""

scenario 18 :
input:
+------------------+
|              word|
+------------------+
|The Social Dilemma|
+------------------+

expected :
+------------------+
|      reverse word|
+------------------+
|ehT laicoS ammeliD|
+------------------+
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

# Create input DataFrame
inputdf = spark.createDataFrame([("The Social Dilemma",)], ["word"])
inputdf.show()

# UDF creation ***IMP
from pyspark.sql.types import StringType
def revString(inputStr):
    return " ".join([word[::-1] for word in inputStr.split(" ")])



udffun=udf(revString, StringType())


inputdf.withColumn("word",udffun(col("word"))).show()




