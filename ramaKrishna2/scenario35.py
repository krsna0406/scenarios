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

df1.select([sum( col(clm).isNull().cast(IntegerType()) ).alias(clm) for clm in df1.columns ]).show()

# trying renaming  *** IMP

df1.select([col(c).alias(c+" REN")  for c in df1.columns ]).show()