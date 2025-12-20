print("welcome mr")


#### ************************  1 reading from input configuration ********************
#
# import sys
# sysargs=sys.argv
#
# print("sysargs",sysargs[1])
#
# filename=sysargs[1]
#
# filename = "C:\\app\\zeyoplus\\misc\\data\\test.txt"
# print(repr(filename))
# try:
#     file=open(filename,"r")
#     for line in file.readlines():
#         print("lines >> ",line)
# except:
#     print("exception")

# with open(filename,"r") as fileObject:
#     for line in fileObject.readlines():
#         print("content  ",line)



#### ************************  2 Checking if a list of columns present in a DataFrame ********************

#
import sys
import os
import urllib.request
import ssl

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################


from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

#create the context,conf and session objects

config=SparkConf().setAppName("practice").setMaster("local[*]")
sc=SparkContext(conf=config)
spark=SparkSession.builder  .getOrCreate()
    #.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:4.0.1") \
#
# df=spark.read.format("csv").option("header",True).load("C:\\app\\zeyoplus\\misc\\pySpa\\sprklingFuture\\delhi_csv.csv")
# df.show()
#
#
# df_cols=df.columns
# check_cols=["COL_CHECK1","col_check1"]
# #//better to create the list in lower first before comparing the columns, and also remove the blanks
#
# lower_list=[i.strip().lower() for i in check_cols]
#
# print(lower_list)
#
# if(all( [check_col in df_cols for check_col in check_cols])):
#     print("YES")
# else:
#     print("NO")

#  3.reading avro by using DSL and SQL

## pyspark --packages org.apache.spark:spark-avro_2.12:3.5.1
## pyspark --packages org.apache.spark:spark-avro_2.12:4.0.1

#df.write.format("avro").save("example")#

# import pyspark
# print(pyspark.__version__)


from pyspark.sql.functions import sequence, lit

df = spark.range(2)
df.show(truncate=False)