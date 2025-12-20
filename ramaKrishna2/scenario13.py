"""

scenario 13 : We have employee id,employee name, department. Need count of every department employees.
input:
+------+--------+-----------+
|emp_id|emp_name|       dept|
+------+--------+-----------+
|     1|    Jhon|Development|
|     2|     Tim|Development|
|     3|   David|    Testing|
|     4|     Sam|    Testing|
|     5|   Green|    Testing|
|     6|  Miller| Production|
|     7|  Brevis| Production|
|     8|  Warner| Production|
|     9|    Salt| Production|
+------+--------+-----------+

expected :


+-----------+-----+
|       dept|total|
+-----------+-----+
|Development|    2|
|    Testing|    3|
| Production|    4|
+-----------+-----+

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
data = [(1, "Jhon", "Development"),
        (2, "Tim", "Development"),
        (3, "David", "Testing"),
        (4, "Sam", "Testing"),
        (5, "Green", "Testing"),
        (6, "Miller", "Production"),
        (7, "Brevis", "Production"),
        (8, "Warner", "Production"),
        (9, "Salt", "Production")]
df = spark.createDataFrame(data, ["emp_id", "emp_name", "dept"])
df.show()

df.groupby("dept").agg(count(col("dept"))).alias("total").show()

# Through SQL
df.createOrReplaceTempView("emptab")
spark.sql("SELECT dept, COUNT(*) AS total FROM emptab GROUP BY dept").show()