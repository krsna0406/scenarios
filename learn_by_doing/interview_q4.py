"""
INTERVIEW QUESTION
WE HAVE 2 TABLE CUSTOMER AND ORDER TABLE IS GIVEN
1. WE HAVE TO FIND OUT CUSTOMER WHO HAVE NOT ORDER ANYTHING
2. WE HAVE TO FIND OUT CUSTOMER WHO HAVE ORDERED
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



customer_data=[(1,'Manish'),(2,'Rahul'),(3,'Monu'),(4,'Ram')]
schema=["Customer_ID", "Customer_Name"]

df_customer = spark.createDataFrame(customer_data,schema)
df_customer.show()

order_data=[(1,4),(3,2)]
schema1=["Order_ID", "Customer_ID"]

df_order = spark.createDataFrame(order_data,schema1)
df_order.show()

print("cutomer who have not ordered")

df_customer.join(df_order,["Customer_ID"],"leftanti").show()

print("cutomer who have ordered")

df_customer.join(df_order,["Customer_ID"],"inner").show()


# by using spark sql

df_customer.createOrReplaceTempView("custdf")
df_order.createOrReplaceTempView("orderdf")


# spark.sql("""
# select custdf.Customer_ID.custdf.Customer_Name from custdf
# left join  orderdf
# on custdf.Customer_ID==orderdf.Customer_ID
# """).show()



print("LEFT ANTI JOIN")
spark.sql("""select * from custdf 
left anti join orderdf on custdf.Customer_ID= orderdf.Customer_ID
""").show()

print("INNER JOIN")
spark.sql("""select * from custdf 
inner  join orderdf on custdf.Customer_ID= orderdf.Customer_ID
""").show()
