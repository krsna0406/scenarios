"""

input:

table a
Emp.No	Name	age	dept	sal	mgr
10	    Ravi	30	it  	98	60
20	    Raja	35	it	    97	40
40	    Mahes	32	it	    97	60
 table b
Emp.No	Name	age	dept	sal	mgr
10	    Ravi	45	it	    98	60
20	    Raja	32	it	    96	40
30	    Rama	45	it	    96	50


output:

Output

+------+-----+---+----+---+---+---------+----------+
|Emp.No| Name|age|dept|sal|mgr|sal_range|age_detail|
+------+-----+---+----+---+---+---------+----------+
|    30| Rama| 45|  it| 96| 50| high_sal|     Older|
|    40|Mahes| 32|  it| 97| 60| high_sal|   Younger|
+------+-----+---+----+---+---+---------+----------+

SQL:


note:

Understanding the Requirement

You have Table A and Table B.
Expected output contains rows not matching between the tables, then create:

sal_range

high_sal

low_sal

age_detail

Older

Younger

This can be solved using FULL OUTER JOIN + COALESCE + CASE logic.

Concepts Tested in Interviews

FULL OUTER JOIN

Finding unmatched records

COALESCE()

Derived columns using when()

Data reconciliation between two tables



"""



# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, round
spark = SparkSession.builder.appName("EmployeeBonus").getOrCreate()


data_a = [
    (10,"Ravi",30,"it",98,60),
    (20,"Raja",35,"it",97,40),
    (40,"Mahes",32,"it",97,60)
]

data_b = [
    (10,"Ravi",45,"it",98,60),
    (20,"Raja",32,"it",96,40),
    (30,"Rama",45,"it",96,50)
]

cols = ["emp_no","name","age","dept","sal","mgr"]

df_a = spark.createDataFrame(data_a, cols)
df_b = spark.createDataFrame(data_b, cols)

from pyspark.sql.functions import *

df = df_a.alias("a").join(
    df_b.alias("b"),
    col("a.emp_no")==col("b.emp_no"),
    "full"
)

result = df.filter(
    col("a.emp_no").isNull() | col("b.emp_no").isNull()
).select(
    coalesce(col("a.emp_no"),col("b.emp_no")).alias("Emp.No"),
    coalesce(col("a.name"),col("b.name")).alias("Name"),
    coalesce(col("a.age"),col("b.age")).alias("age"),
    coalesce(col("a.dept"),col("b.dept")).alias("dept"),
    coalesce(col("a.sal"),col("b.sal")).alias("sal"),
    coalesce(col("a.mgr"),col("b.mgr")).alias("mgr")
).withColumn(
    "sal_range",
    when(col("sal") >= 96,"high_sal").otherwise("low_Sal")
).withColumn(
    "age_detail",
    when(col("age") >= 40,"Older").otherwise("Younger")
)

result.show()

