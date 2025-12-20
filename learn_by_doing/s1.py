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
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

# flatten the data

# data = [
#     ("Manish",["Java","Scala","C++"]),
#     ("rahul",["Spark","Java","C++","Spark","Java"]),
#     ("shyam",["CSharp","VB","Spark","Python"])
# ]

data = [
    ("Manish","Java,Scala,C++"),
    ("rahul","Spark,Java,C++,Spark,Java"),
    ("shyam","CSharp,VB,Spark,Python")
]
columns=["name","language"]

df = spark.createDataFrame(data,columns)
df.show(truncate=False)
df.printSchema()



# df.withColumn("language" ,expr("explode(split(language, ','))")).show()
#
# # df.withColumn("language" , explode(split("language",","))).show()
# df.withColumn("language" , explode(split(col("language"),","))).show()


print("spark SQL")

df.createOrReplaceTempView("df")


spark.sql("""
select name, explode(split(language,",")) as language from df
""").show(truncate=False)


