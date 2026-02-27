import sys
import os
import urllib.request
import ssl

from pyspark.sql.functions import expr, col, split
from pyspark.sql.types import IntegerType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################




from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import  *

conf=SparkConf().setAppName("word count").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

data = [
    ("Hello world",),
    ("Hello Spark",),
    ("Hello Python Spark",),
    ("Big data with Spark and Python",),
    ("World of data and analytics",)
]
#
# schema=[("result")]
#
#
# df=spark.createDataFrame(data,schema)
# # df.show(truncate=False)
#
# sdf=df.withColumn("word",split(col("result")," "))
#
# # sdf.show(truncate=False)
#
# sdf2=sdf.withColumn("word",expr(" explode( word) ")).drop("result").select(lower(col("word")).alias("word"))
#
# sdf2.show(truncate=False)
#
#
# sdf2.groupby("word").agg(count("*")).show()

#
# rdd=sc.parallelize(data)
# rdd.foreach(lambda  x: print(x))
#
# rdd1=rdd.flatMap(lambda x: x[0].lower().split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
# # rdd1.collect()
# # rdd1.foreach(lambda  x: print(x))
# for x in rdd1.collect():
#     print(x)

#
# data = [
#     (1, "English", 90, 2024),
#     (1, "Maths", 91, 2024),
#     (1, "Physics", 92, 2024),
#     (1, "Chemistry", 93, 2024),
#     (1, "Physics", 98, 2025),
#     (1, "Chemistry", 87, 2025),
#     (1, "English", 92, 2025),
#     (1, "Maths", 95, 2025),
#     (1, "Biology", 70, 2024)
# ]
#
# columns = ["StudentId", "Subject", "Marks", "Year"]
# df = spark.createDataFrame(data, columns)
# df.show()
#
# df.groupby("StudentId","year").pivot("Subject").agg(sum("Marks")).show()

#
# data = [
#     (1, "HR", 3000),
#     (2, "IT", 3500),
#     (3, "admin", 4000),
#     (4, "HR", 5000),
#     (5, "admin", 6500)
# ]
#
# rdd = sc.parallelize(data)
#
# # Step 1: Map → (department, (salary, 1))
# dept_salary = rdd.map(lambda x: (x[1], (x[2], 1)))
#
# # Step 2: ReduceByKey → (department, (total_salary, total_count))
# dept_sum_count = dept_salary.reduceByKey(
#     lambda a, b: (a[0] + b[0], a[1] + b[1])
# )
#
#
# dept_sum_count.foreach(print)
# # Step 3: Calculate average
# dept_avg = dept_sum_count.mapValues(
#     lambda x: x[0] / x[1]
# )
#
# dept_avg.collect()

#
# data = [1, [2, 3], [4, [5, 6]], 7]
#
# def flat_data(inputdata):
#     result=[]
#
#     for item in inputdata:
#         if isinstance(item,list):
#             result.extend(flat_data(item))
#         else:
#             result.append(item)
#     return result
#
# print("flat data is"  , flat_data(data))


#Given a list of numbers, return the second highest element.

#
# data = [10, 20, 30, 40, 50,60]
# def second_highest(arr):
#     if len(arr) < 2:
#         return None
#
#     highest = arr[0]
#     second = None
#
#     for i in range(1, len(arr)):
#         if arr[i] > highest:
#             second = highest
#             highest = arr[i]
#         elif arr[i] < highest:
#             if second is None or arr[i] > second:
#                 second = arr[i]
#
#     return second
#
#
# print(second_highest(data))

# a = 1,2
# print(a)


# def fib():
#     a,b=0,1
#     while True:
#         yield a
#         a,b=b,a+b
#
# gen=fib()
#
# for i in gen:
#     if i<100:
#         print(i)
#

# fibonacci series



# a=[1,2,3,4,5,6]
#
# a[0],a[-1]=a[-1],a[0]
# print(a)
#
# dict={'Sun':5,'Mon':3,'Tue':5,'Wed':3}
# a={}
#
# for key,value in dict.items():
#     print(key,"",value)
#     if value not in a:
#         a[value]=key # { 5,Sun }
#     else:
#         a[value].append(key)
#
# print(a)
#
#
#


#
# from pyspark.storagelevel import  StorageLevel



# checking the RDD

# data=[("fdfdfd fdfdfd fdfdfd ",),("fdfdfd fdfdfd fdfdfd",)]
#
# rdd=sc.parallelize(data)
#
# # rdd.foreach(print)
#
# rdd2=rdd.flatMap(lambda x: x[0].split()).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)
#
#
# print(rdd2.collect()[0][0],rdd2.collect()[0][1])
# rdd2.foreach(print)
print(spark.conf.get("spark.sql.autoBroadCastJoinThresold"))

