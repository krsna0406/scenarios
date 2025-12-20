"""

scenario1: Query to get who are getting equal salary

input:

+--------+---------+--------+------+-------------------+------+
|workerid|firstname|lastname|salary|        joiningdate|depart|
+--------+---------+--------+------+-------------------+------+
|     001|   Monika|   Arora|100000|2014-02-20 09:00:00|    HR|
|     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
|     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
|     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
|     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
+--------+---------+--------+------+-------------------+------+

expected

+--------+---------+--------+------+-------------------+------+
|workerid|firstname|lastname|salary|        joiningdate|depart|
+--------+---------+--------+------+-------------------+------+
|     002| Niharika|   Verma|300000|2014-06-11 09:00:00| Admin|
|     003|   Vishal| Singhal|300000|2014-02-20 09:00:00|    HR|
|     004|  Amitabh|   Singh|500000|2014-02-20 09:00:00| Admin|
|     005|    Vivek|   Bhati|500000|2014-06-11 09:00:00| Admin|
+--------+---------+--------+------+-------------------+------+

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


data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]
df = spark.createDataFrame(data,schema=myschema)
df.show()

# hint use self inner join

# df_1=df.alias("df1")
# print("DUPLICATED BY ALIAS")
# df_1.show()

#
# joindf=df.join(df_1,((df["workerid"]!=df_1["workerid"]) & (df["salary"]==df_1["salary"])),"inner")
# joindf.show()
# print(joindf.columns)
# print(list(set(df.columns)))
#
# # joindf.select(*set(df.columns)).show()
#
# joindf.select("df.workerid").show()
print("DSL")

#***imp
df.alias("a").join(broadcast(df.alias("b")),((col("a.workerid")!=col("b.workerid")) & (col("a.salary")==col("b.salary"))),"inner")\
    .select(col("a.workerid"),col("a.firstname"),col("a.lastname"),col("a.salary"),col("a.joiningdate"),col("a.depart"))\
    .show(truncate=False)


print("SPARK SQL")


df.createOrReplaceTempView("sqldf")

spark.sql("""

select a.workerid,a.firstname,a.lastname,a.salary,a.joiningdate,a.depart
from sqldf a,sqldf b 
where a.workerid!=b.workerid
and a.salary=b.salary
""").show(truncate=False)



