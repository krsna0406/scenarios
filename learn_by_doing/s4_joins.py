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

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

# CSV READS


empdf=spark.read.format("csv").option("header",True).load("C:\\app\\zeyoplus\\practice\\learnByDoing\\pysparkHandson\\source\\employee_data.csv")
empdf.show()

deptdf=spark.read.format("csv").option("header",True).load("C:\\app\\zeyoplus\\practice\\learnByDoing\\pysparkHandson\\source\\department_data.csv")
deptdf.show()

joindf=empdf.join(deptdf,empdf.emp_id== deptdf.emp_id , "fullouter")
joindf.show()


#
# joindf1=empdf.join(deptdf,["emp_id"] , "inner")
# joindf1.show()