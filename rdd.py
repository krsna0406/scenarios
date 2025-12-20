import sys
import os
import urllib.request
import ssl

from pyspark.sql.functions import substring, col

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################


from pyspark.sql import  SparkSession
from pyspark import SparkContext,SparkConf


# from pyspark.sql.functions import *

conf = SparkConf().setAppName("pyspark").setMaster("local[*]")
sc= SparkContext(conf=conf) # here conf=conf is important
spark=(SparkSession.builder.master("local[*]").appName("rdd").getOrCreate())
# #
# rdd=sc.textFile("D:\\CDOWNLOADS\\azure data factory dataset\\delhi_csv.csv")
# rows = rdd.map(lambda x: x.split(","))
# df = rows.toDF(["month","sales","profit","location"])
# df .show()
#
# for x in rdd.collect():
#     print(x)
#
# print("STARTED THE PYSPARK")





# Create Sample Data
data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df=spark.createDataFrame(data,columns)

# Using substring()
df1=df.withColumn('year', substring("date", 1,4)) \
    .withColumn('month', substring('date', 5,2)) \
    .withColumn('day', substring('date', 7,2))
df1.printSchema()
df1.show(truncate=False)


df2=df1.withColumn('newcol',df1["year"]+"123")
df2.show()

input("enter any key to exit")