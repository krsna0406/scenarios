"""

scenario 25 :
onsider a file with some bad/corrupt data as shown below.How will you handle those and load into spark dataframe
Note - avoid using filter after reading as DF and try to remove bad data while reading the file itself

input:
emp_no,emp_name,dep
101,Murugan,HealthCare
Invalid Entry,Description: Bad Record Entry
102,Kannan,Finance
103,Mani,IT
Connection lost,Description: Poor Connection
104,Pavan,HR
Bad Record,Description:Corrupt Record

expected :

+------+--------+----------+
|emp_no|emp_name|       dep|
+------+--------+----------+
|   101| Murugan|HealthCare|
|   102|  Kannan|   Finance|
|   103|    Mani|        IT|
|   104|   Pavan|        HR|
+------+--------+----------+



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
from pyspark.sql.functions import repeat, col

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()

df=spark.read.format("csv").option("header","true")\
    .option("mode","dropmalformed").load("C:\\Users\\Krishna\\IdeaProjects\\kanna\\ramaKrishna2\\resource\\s25.csv")
df.show()

