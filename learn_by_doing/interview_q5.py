"""
INTERVIEW QUESTION
WE HAVE 2 TABLE EMPLOYEE AND DEPARTMENT TABLE IS GIVEN
1. WE HAVE TO FIND THE HIGHEST SALARY BASED ON EACH DEPARTMENT NAME
2. WE HAVE TO FIND THE EMPLOYEE WHO IS GETTING HIGHEST SALARY BASED ON EACH DEPARTMENT NAME
3. WE HAVE TO FIND THE lowest SALARY BASED ON EACH DEPARTMENT NAME
4. WE HAVE TO FIND THE EMPLOYEE WHO IS GETTING lowest SALARY BASED ON EACH DEPARTMENT NAME
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

emp_data=[('Manish' , 1 , 75000),
          ('Raghav' , 1 , 85000 ),
          ('surya' , 1 , 80000 ),
          ('virat' , 2 , 70000),
          ('rohit' , 2 , 75000),
          ('jadeja' , 3 , 85000),
          ('anil' , 3 , 55000),
          ('sachin' , 3 , 55000),
          ('zahir', 4, 60000),
          ('bumrah' , 4 , 65000) ]
schema= ["emp_name" ,"dept_id" ,"salary"]

emp_df=spark.createDataFrame(emp_data,schema)
emp_df.show()


dept_data = [(1, 'DATA ENGINEER'),(2, 'SALES'),(3, 'SOFTWARE'),(4, 'HR')]
schema1=['dept_id','dept_name']

dept_df=spark.createDataFrame(dept_data,schema1)
dept_df.show()

#joineddf=emp_df.join(dept_df,["dept_id"],"inner").show()

window=Window.partitionBy("dept_id").orderBy(col("salary").desc())

emp_df.withColumn("hrank",rank().over(window)).show()

hdf=emp_df.withColumn("hrank",rank().over(window)).filter(col("hrank")==1)
print("HIGHEST ")
hdf.join(dept_df,["dept_id"],"inner").show()

window_low=Window.partitionBy("dept_id").orderBy(col("salary").asc())

emp_df.withColumn("lrank",rank().over(window_low)).show()
ldf=emp_df.withColumn("lrank",rank().over(window_low)).filter(col("lrank")==1)

print("LOWEST ")
ldf.join(dept_df,["dept_id"],"inner").show()