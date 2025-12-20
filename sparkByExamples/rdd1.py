"""
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
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,56])
rdd.foreach(print)

#empty RDD creation
#
# erdd=sc.parallelize([])
# print(erdd.count())
#
#
# emptyRDD=sc.emptyRDD()
# print("RDD size {0}",{emptyRDD.count()+3})
#
# print("RDDqqqqq size {0}".format(emptyRDD.count()))

#
#
# data = ["Project",
#         "Gutenberg’s",
#         "Alice’s",
#         "Adventures",
#         "in",
#         "Wonderland",
#         "Project",
#         "Gutenberg’s",
#         "Adventures",
#         "in",
#         "Wonderland",
#         "Project",
#         "Gutenberg’s"]
#
# rdd=spark.sparkContext.parallelize(data)
#
# rdd2=rdd.map(lambda x: (x,1))
#
# rdd3=rdd2.reduceByKey(lambda x,y: x+y).collect()
#
# for row in rdd3:
#     print(">>>>>>>>>>>>>>>>>",row[0],"------------",row[1])


data = ["Project",
        "Gutenberg’s",
        "Alice’s",
        "Adventures",
        "in",
        "Wonderland",
        "Project",
        "Gutenberg’s",
        "Adventures",
        "in",
        "Wonderland",
        "Project",
        "Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)

data = [('James','Smith','M',30),
        ('Anna','Rose','F',41),
        ('Robert','Williams','M',62),
        ]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

rdd2=df.rdd.map(lambda x:
                (x[0]+","+x[1],x[2],x[3]*2)
                )
df2=rdd2.toDF(["name","gender","new_salary"]   )
df2.show()


#Referring Column Names
rdd2=df.rdd.map(lambda x:
                (x["firstname"]+","+x["lastname"],x["gender"],x["salary"]*2)
                )


#Referring Column Names
rdd2=df.rdd.map(lambda x:
                (x.firstname+","+x.lastname,x.gender,x.salary*2)
                )


def func1(x):
    firstName=x.firstname
    lastName=x.lastname
    name=firstName+","+lastName
    gender=x.gender.lower()
    salary=x.salary*2
    return (name,gender,salary)

rdd2=df.rdd.map(lambda x: func1(x)).toDF().show()
rdd2=df.rdd.map(func1).toDF().show()