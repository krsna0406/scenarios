"""
INTERVIEW QUESTION
1. Group multiple rows into single

SOLVE USING PYSPARK AND SPARK SQL
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

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

data=[(1,'Manish','Mobile'),(1,'Manish','Washing Mavhine'),(2,'Rahul','Car'),(2,'Rahul','mobile'),(2,'Rahul','scooty'),(3,'Monu','Scooty')]
schema=["Customer_ID", "Customer_Name",'Purchase']

df = spark.createDataFrame(data,schema)
df.show()

df.groupby("Customer_ID","Customer_Name").agg(collect_set(col("Purchase")).alias("Purchase")).show(truncate=False)


df.createOrReplaceTempView("sqldf")
print("===========BY USING SPARK SQL=====================")
spark.sql("""
select Customer_ID,Customer_Name,collect_set(Purchase) as Purchase
from sqldf group by Customer_ID,Customer_Name
""").show(truncate=False)