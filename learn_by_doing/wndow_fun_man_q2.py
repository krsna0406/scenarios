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

from pyspark.sql import  Window

from pyspark.sql.functions import *

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

emp_data = [(1,"manish","11-07-2023","10:20"),
            (1,"manish","11-07-2023","11:20"),
            (2,"rajesh","11-07-2023","11:20"),
            (1,"manish","11-07-2023","11:50"),
            (2,"rajesh","11-07-2023","13:20"),
            (1,"manish","11-07-2023","19:20"),
            (2,"rajesh","11-07-2023","17:20"),
            (1,"manish","12-07-2023","10:32"),
            (1,"manish","12-07-2023","12:20"),
            (3,"vikash","12-07-2023","09:12"),
            (1,"manish","12-07-2023","16:23"),
            (3,"vikash","12-07-2023","18:08")]

emp_schema = ["id", "name", "date", "time"]
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

emp_df.show()




window=Window.partitionBy("id","date").orderBy("time")\
    .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

windf=emp_df.withColumn("min_time",to_timestamp(min("time").over(window), "HH:mm"))\
    .withColumn("max_time",to_timestamp(max("time").over(window), "HH:mm"))

windf.show()
windf.printSchema()

timedf=windf.withColumn("duration",(col("max_time").cast("integer") - col("min_time").cast("integer"))/3600)

timedf.show()

finaldf=timedf.select("id","name","date","duration").distinct().filter(col("duration")<8)
finaldf.show()


