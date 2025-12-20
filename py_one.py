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


from pyspark import SparkContext, SparkConf
from pyspark.sql import  SparkSession
from pyspark.sql.functions import desc, collect_set

conf=SparkConf().setAppName("practice").setMaster("local[*]")
sc= SparkContext(conf=conf)
sc.setLogLevel("error")

spark=SparkSession.builder.getOrCreate()

# CSV READ
#
# df =spark.read.csv("df.csv",header=True,inferSchema=True)
# # df.fillna("NA").show()
# df.orderBy("id").show()
#
# # df.printSchema()
#
# df1 =spark.read.format("csv").option("header",True).load("df.csv")
# df1.show()

# df1.orderBy(desc("id")).show()

#
# ##############################  JSON READ ##########################
# df=spark.read.option("multiline",False).json("file4.json")
# df.show()
#
# df1=spark.read.json("file4.json")
# df1.show(10,truncate=False)
#
# print("!!!!!!!!!!!!!!@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
# df.select(df.columns[::-1]).show()


#Group multiple rows into single

#
# data=[(1,'Manish','Mobile'),(1,'Manish','Washing Mavhine'),(2,'Rahul','Car'),(2,'Rahul','mobile'),(2,'Rahul','scooty'),(3,'Monu','Scooty')]
# schema=["Customer_ID", "Customer_Name",'Purchase']
#
# df = spark.createDataFrame(data,schema)
# df.show()
#
# df.createOrReplaceTempView("tempdf")
#
# df2=spark.sql("select Customer_ID,Customer_Name,collect_set(Purchase) from tempdf group by Customer_ID,Customer_Name")
#
# df2.show(truncate=False)
#
# df3=df.groupby("Customer_ID","Customer_Name").agg(collect_set("Purchase").alias("purchase"))
# df3.show(truncate=False)
#
# #  how to combine many list
#
# list1 = ["a", "b", "c", "d"]
# list2 = [1, 2, 3, 4]
#
# combinedlist=list(zip(list1,list2))
# rdd=sc.parallelize(combinedlist)
# rdd.toDF(["one","two"]).show(truncate=False) #here in toDF list has to be passed.
#
#

# filter

#
# from pyspark.sql.functions import *
#
# df=spark.read.csv("df.csv" ,header=True,inferSchema=True)
#
# df.show()
# df.filter(df.category=='Exercise').show()
# df.filter(col("category")=='Exercise').show()
# df.filter(df["category"]=='Exercise').show()
#
# df.filter(df.category.like("%Gy%")).show() # & |
#
# from pyspark.sql.functions import  *
#
# df=spark.read.csv("df.csv" ,header=True,inferSchema=True)
# df.printSchema()
#
# df.show()
#
# df2=df.withColumn("product",col("product").cast(IntegerType()))
#
# df2.printSchema()
# df2.na.fill("").show()



df=spark.read.csv("sample2.csv",header=True,inferSchema=True)
df.show()
df.drop_duplicates().show()
df.drop_duplicates(["salary"]).show()
df.sort(desc("salary")).show()


print(df.rdd.getNumPartitions())

df.orderBy(["salary"]).show()

