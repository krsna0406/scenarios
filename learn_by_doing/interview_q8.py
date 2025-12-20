"""

INTERVIEW QUESTION
1. Find out the missing number
SOLVE USING PYSPARK AND SPARK SQL

"""

print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, StructField

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
from pyspark.sql.functions import *
#
# data=[(1,),(2,),(5,),(7,),(8,),(10,)]
# column=['ID']
# df=spark.createDataFrame(data,column)
# df.show()
#
# from pyspark.sql.types import IntegerType
# list_new= range(1,11,1)
# df_new=spark.createDataFrame(list_new,IntegerType())
# df_new.show()
#
# df_new.subtract(df).show()
#


# below is for practice

data=range(1,20,2)

df2=spark.createDataFrame(data,"integer")
df2.show()
df2.printSchema()

df3=df2.withColumn("sysdate",current_date())\
 .withColumn("time",current_timestamp())

df3.show(truncate=False)

df3.printSchema()


df4=df3.withColumn("to_timestamp",to_timestamp(col("sysdate")))\
    .withColumn("to_date",to_date(col("time")))
df4.show()


