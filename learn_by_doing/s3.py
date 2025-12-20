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

# CSV READS


df=spark.read.format("csv").option("header",True).load("C:\\app\\zeyoplus\\practice\\learnByDoing\\pysparkHandson\\source\\employees.csv")
df.show()

df.withColumn("loc",coalesce("loc",lit(""))).show()

print("""     "colName"    """)
df.select("emp_id","name").show()

print("     DF.colName            ")

df.select(df.emp_id,df.name).show()

print("""     "col("colname")"    """)

df.select(col("emp_id"),col("name")).show()


