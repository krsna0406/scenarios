"""
scenario:

Find out the performance of the sales based on last three months average


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

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
from pyspark.sql import  Window

product_data = [
    (1,"iphone","01-01-2023",1500000),
    (2,"samsung","01-01-2023",1100000),
    (3,"oneplus","01-01-2023",1100000),
    (1,"iphone","01-02-2023",1300000),
    (2,"samsung","01-02-2023",1120000),
    (3,"oneplus","01-02-2023",1120000),
    (1,"iphone","01-03-2023",1600000),
    (2,"samsung","01-03-2023",1080000),
    (3,"oneplus","01-03-2023",1160000),
    (1,"iphone","01-04-2023",1700000),
    (2,"samsung","01-04-2023",1800000),
    (3,"oneplus","01-04-2023",1170000),
    (1,"iphone","01-05-2023",1200000),
    (2,"samsung","01-05-2023",980000),
    (3,"oneplus","01-05-2023",1175000),
    (1,"iphone","01-06-2023",1100000),
    (2,"samsung","01-06-2023",1100000),
    (3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)
product_df.show()


window=Window.partitionBy("product_id").orderBy("sales_date").rowsBetween(-2,0)

product_df.withColumn("running_sum",sum("sales").over(window))\
    .withColumn("average",col("running_sum")/3).show()

#NOTE: REMOVE FIRST TWO RECORDS VERY IMP

product_df.withColumn("running_sum",sum("sales").over(window)) \
    .withColumn("average",col("running_sum")/3)\
    .withColumn("rownum",row_number().over(Window.partitionBy("product_id").orderBy("sales_date")))\
    .filter(col("rownum")>2)\
    .withColumn("performace",expr(" case when  sales < average then 'BAD' else 'GOOD' end"))\
    .show()

