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

data=[(1,'manish','india',10000),(2,'rani','india',50000),(3,'sunny','UK',5000),(4,'sohan','UK',25000),(5,'mona','india',10000),(6,'teena','india',5000)]
columns=['id','name','country','salary']

df =spark.createDataFrame(data,columns)
df.show()

# window functions by DSL and Spark SQL

from pyspark.sql import  Window

window_spec=Window.partitionBy("country").orderBy(col("salary").desc())

df.withColumn("rowno",row_number().over(window_spec)) \
    .withColumn("rank",rank().over(window_spec)) \
.withColumn("d_rank",dense_rank().over(window_spec)) \
    .show()


    # by using spark SQL

# print("window fun by SPARK SQL")
# df.createOrReplaceTempView("sqldf")
# spark.sql("""
# select * ,
# row_number() over ( partition by country order by salary  desc ) as rownum from sqldf""").show()
#

