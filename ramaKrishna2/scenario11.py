"""

scenario 11 :

I have a table called Emp_table, it has 3 columns, Emp name, emp ID , salary
in this I want to get salaries that are >10000 as Grade A, 5000-10000 as grade B and < 5000 as Grade C,
write an SQL query)

input:
+------+---------------+------+
|emp_id|       emp_name|salary|
+------+---------------+------+
|     1|           Jhon|  4000|
|     2|      Tim David| 12000|
|     3|Json Bhrendroff|  7000|
|     4|         Jordon|  8000|
|     5|          Green| 14000|
|     6|         Brewis|  6000|
+------+---------------+------+

expected :
+------+---------------+------+-----+
|emp_id|       emp_name|salary|grade|
+------+---------------+------+-----+
|     1|           Jhon|  4000|    C|
|     2|      Tim David| 12000|    A|
|     3|Json Bhrendroff|  7000|    B|
|     4|         Jordon|  8000|    B|
|     5|          Green| 14000|    A|
|     6|         Brewis|  6000|    B|
+------+---------------+------+-----+

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
data = [
    (1, "Jhon", 4000),
    (2, "Tim David", 12000),
    (3, "Json Bhrendroff", 7000),
    (4, "Jordon", 8000),
    (5, "Green", 14000),
    (6, "Brewis", 6000)
]
df = spark.createDataFrame(data, ["emp_id", "emp_name", "salary"])
df.show()

print("BY expr")

df.withColumn("grade",expr("""
case when salary>= 10000 then 'A' 
when (salary>= 5000  and salary< 10000) then 'B' 
else 'C'
end
""")).show()

print("DSL")

df.withColumn("grade",when(col("salary")>=10000, 'A').when( ( (col("salary")>5000) & (col("salary")<10000)) ,'B').otherwise('C')).show()

print("SPARK SQL ")
df.createOrReplaceTempView("sqldf")
spark.sql("""
select * , case when salary>= 10000 then 'A' 
when (salary>= 5000  and salary< 10000) then 'B' 
else 'C'
end grade from sqldf
""").show()