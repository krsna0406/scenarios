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

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
dept = [("Finance",10), \
        ("Marketing",20), \
        ("Sales",30), \
        ("IT",40) \
        ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)
deptDF.printSchema()

#define the function and register and use it
from pyspark.sql.types import  *

def square(x):
        return x*x

squarudf=udf(square,LongType())


deptDF.withColumn("dept_id_sq",squarudf("dept_id") ).show()