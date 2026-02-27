"""

scenario 36:

Create a new datafrane df1 with the given values
Count null entries in a datafarme
Remove null entries and the store the null entries in a new datafarme df2
Create a new dataframe df3 with the given values and join the two dataframes df1 & df2
Fill the null values with the mean age all of students
Filter the students who are 18 years above and older

input:
expected :

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

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()

#creating the dataframe df1
data1 = [(1,'Jhon',17),(2,'Maria',20),(3,'Raj',None),(4,'Rachel',18)]
columns = ["id","name","age"]
df1 = spark.createDataFrame(data1,columns)
df1.show()


print("AAAAAAAAAAAAAAAAA")

# df1.select([ col(clm).isNull().cast(IntegerType() ).alias(clm) for clm in df1.columns ]).show()


df1.select([sum( col(clm).isNull().cast(IntegerType()) ).alias(clm) for clm in df1.columns ]).show()

# trying renaming  *** IMP

# df1.select([col(c).alias(c.replace("id","KRISHNA")+" REN")  for c in df1.columns ]).show()

#Remove the row with null entires and store them in a new dataframe named df2
df2 = df1.filter(col("age").isNull())
df2.show()


#create a new dataframe df3
data2 = [(1,'seatle',82),(2,'london',75),(3,'banglore',60),(4,'boston',90)]
columns2 = ["id","city","code"]

df3 = spark.createDataFrame(data2,columns2)
df3.show()

mergedf = df1.join(df3, df1["id"]==df3["id"],"inner").select(df1["id"],"name","age","city","code")
mergedf.show()


#fill the null value with the mean age of students
#calculate the mean age

print("mean --by using collect")
meanage = mergedf.select(round(mean("age"))).collect()[0][0]
print(meanage)

print("mean --by using first")


meanage1 = mergedf.select(round(mean("age"))).first()[0]
print(meanage1)

filldf = mergedf.na.fill({"age":meanage})
filldf.show()



#Get the students who are 18 years or older
filterdf = filldf.filter(col("age")>= 18)
filterdf.show()

