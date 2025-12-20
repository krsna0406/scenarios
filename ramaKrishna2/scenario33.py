"""

scenario 33: Write a query to print the maximum number of discount tours any 1 family can choose.

input:

+--------------------+--------------+-----------+
|                  id|          name|family_size|
+--------------------+--------------+-----------+
|c00dac11bde74750b...|   Alex Thomas|          9|
|eb6f2d3426694667a...|    Chris Gray|          2|
|3f7b5b8e835d4e1c8...| Emily Johnson|          4|
|9a345b079d9f4d3ca...| Michael Brown|          6|
|e0a5f57516024de2a...|Jessica Wilson|          3|
+--------------------+--------------+-----------+

+--------------------+------------+--------+--------+
|                  id|        name|min_size|max_size|
+--------------------+------------+--------+--------+
|023fd23615bd4ff4b...|     Bolivia|       2|       4|
|be247f73de0f4b2d8...|Cook Islands|       4|       8|
|3e85ab80a6f84ef3b...|      Brazil|       4|       7|
|e571e164152c4f7c8...|   Australia|       5|       9|
|f35a7bb7d44342f7a...|      Canada|       3|       5|
|a1b5a4b5fc5f46f89...|       Japan|      10|      12|
+--------------------+------------+--------+--------+

expected :

+-------------+-------------------+
|         name|number_of_countries|
+-------------+-------------------+
|Emily Johnson|                  4|
+-------------+-------------------+

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

data = [('c00dac11bde74750b4d207b9c182a85f', 'Alex Thomas', 9),('eb6f2d3426694667ae3e79d6274114a4', 'Chris Gray', 2),('3f7b5b8e835d4e1c8b3e12e964a741f3', 'Emily Johnson', 4),('9a345b079d9f4d3cafb2d4c11d20f8ce', 'Michael Brown', 6),('e0a5f57516024de2a231d09de2cbe9d1', 'Jessica Wilson', 3)]

familydf = spark.createDataFrame(data,["id","name","family_size"])
familydf.show()

countrydata = [('023fd23615bd4ff4b2ae0a13ed7efec9', 'Bolivia', 2 , 4),('be247f73de0f4b2d810367cb26941fb9', 'Cook Islands', 4,8),('3e85ab80a6f84ef3b9068b21dbcc54b3', 'Brazil', 4,7),('e571e164152c4f7c8413e2734f67b146', 'Australia', 5,9),('f35a7bb7d44342f7a8a42a53115294a8', 'Canada', 3,5),('a1b5a4b5fc5f46f891d9040566a78f27', 'Japan', 10,12)]

countrydf = spark.createDataFrame(countrydata,["id","name","min_size","max_size"])
countrydf.show()

#join as family size >minsize and < max size

joindf=familydf.join(countrydf,(familydf["family_size"] >= countrydf["min_size"]) & ( familydf["family_size"] <= countrydf["max_size"]) ,"inner")
joindf1=joindf.select(familydf["name"],familydf["family_size"],countrydf["name"],"min_size","max_size")

joindf1.printSchema()
joindf2=joindf1.groupby(familydf["name"]).agg (count(col("*")).alias("count"))

joindf2.agg(max(col("count"))).show()


# can be done by using rank aswell

joindf2.show()

print("BY USING WINDOW FUN")
from pyspark.sql import Window
window=Window.orderBy(col("count").desc())

withWinDF=joindf2.withColumn("rank",rank().over(window))

withWinDF.filter(col("rank")==1).show()



