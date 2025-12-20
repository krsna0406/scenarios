"""

scenario 26 :
input:

+---+----+
| id|name|
+---+----+
|  1|   A|
|  2|   B|
|  3|   C|
|  4|   D|
+---+----+

+---+-----+
|id1|name1|
+---+-----+
|  1|    A|
|  2|    B|
|  4|    X|
|  5|    F|
+---+-----+

expected :

+---+-------------+
| id|      comment|
+---+-------------+
|  3|new in source|
|  4|     mismatch|
|  5|new in target|
+---+-------------+
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


sourcedata = [
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")]
mysourceshcema = ["id","name"]
sourcedf = spark.createDataFrame(sourcedata,schema=mysourceshcema)
sourcedf.show()

targetdata = [
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")]
mytargetschema = ["id1","name1"]
targetdf = spark.createDataFrame(targetdata,schema=mytargetschema)
targetdf.show()


#--------------------------Through SQL

sourcedf.createOrReplaceTempView("sourcetab")
targetdf.createOrReplaceTempView("targettab")

print("=================Through SQL==========================")
spark.sql("""SELECT COALESCE(s.id, t.id1) AS id,
       CASE
           WHEN s.name IS NULL THEN 'new in target'
           WHEN t.name1 IS NULL THEN 'new in source'
           WHEN s.name != t.name1 THEN 'mismatch'
       END AS comment
FROM sourcetab s
FULL OUTER JOIN targettab t ON s.id = t.id1
WHERE s.name != t.name1 OR s.name IS NULL OR t.name1 IS NULL
""").show()

joindf=sourcedf.join(targetdf, (sourcedf.id== targetdf.id1) ,"outer")


joindf1=joindf.filter((col("name")!=col("name1")) |col("name").isNull() | col("name1").isNull())

joindf1.withColumn("comment",
                  when((col("id1")).isNull(),"new in source")
                  .when((col("id")).isNull(),"new in target")
                  .otherwise("mismatch")
                  )\
    .withColumn("id",coalesce(col("id"),col("id1"))).drop("id1").show()