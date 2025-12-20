import sys
import os
import urllib.request
import ssl

from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################


from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.window import  *

config=SparkConf().setAppName("window-funs").setMaster("local[*]")
sc=SparkContext(conf=config)
spark=SparkSession.builder.getOrCreate()

data=[(1,'manish','india',10000),(2,'rani','india',50000),(3,'sunny','UK',5000),(4,'sohan','UK',25000),(5,'mona','india',10000)]
columns=['id','name','country','salary']

df =spark.createDataFrame(data,columns)
df.show()

# rownum.rank and dence rank

window=Window.orderBy("salary")

df1=df.withColumn("rownum",row_number().over(window)).withColumn("rank",rank().over(window)).\
    withColumn("dense_rank",dense_rank().over(window)).\
    withColumn("sum--",sum("salary").over(window))
df1.show()


