"""
INTERVIEW QUESTION
1 question
1. WE are getting data in the form of string JSON we need to convert into json format
2. After converting we need to create seperate column from the json body.

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
data = [(1,['mobile','PC','Tab']),(2,['mobile','PC']),(3,['Tab','Pen'])]
schema=['customer_id','product_purchase']


data=[('Manish','{"street": "123 St", "city": "Delhi"}'),('Ram','{"street": "456 St", "city": "Mumbai"}')]
schema=['name','address']
df=spark.createDataFrame(data,schema)
print(df.printSchema())
df.show(truncate=False)


#DSL


schema=  StructType([
                     StructField(name='street',dataType=StringType()),
                     StructField(name='city',dataType=StringType())]
                    )

fdf=df.withColumn("temp", from_json(col("address"),schema))
fdf.show()

fdf.printSchema()

fdf.select("*","temp.*").show()


print("--------------SPARK SQL-------------")

df.createOrReplaceTempView("sample")

spark.sql("""
with cte as(
        select name,address,from_json(address,'street string,city string') as address_new from sample
)
select name,address,address_new,address_new.street as street,address_new.city as city from cte
""").show()

