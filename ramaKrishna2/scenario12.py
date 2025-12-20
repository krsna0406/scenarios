"""

scenario 12 :

input:
+--------------------+----------+
|               email|    mobile|
+--------------------+----------+
|Renuka1992@gmail.com|9856765434|
|anbu.arasu@gmail.com|9844567788|
+--------------------+----------+
expected :

+--------------------+----------+
|               email|    mobile|
+--------------------+----------+
|R**********92@gma...|98*****434|
|a**********su@gma...|98*****788|
+--------------------+----------+



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


#creating UDF functions for masked data, here email[0] is it will take first letter i.e 0th index and email[8:] is it will take the string from 8th index position to end of the string
def mask_email(email):
        return (email[0] + "**********" + email[8:])

#creating UDF functions for masked data, here mobile[0:2] is it will take string from Index 0 to 2 letters and mobile[-3:] is it will take string last three index to end the end of the string
def mask_mobile(mobile):
        return (mobile[0:2] + "*****" + mobile[-3:])


df = spark.createDataFrame([("Renuka1992@gmail.com", "9856765434"), ("anbu.arasu@gmail.com", "9844567788")], ["email", "mobile"])
df.show()

maskeddf = df.withColumn("email",udf(mask_email)(df.email)).withColumn("mobile",udf(mask_mobile)(df.mobile))
maskeddf.show()