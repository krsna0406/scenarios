"""

scenario4: (Write a query to list the unique customer names in the custtab table, along with the number of addresses associated with each customer.)

input:
+------+-----------+-------+
|custid|   custname|address|
+------+-----------+-------+
|     1|   Mark Ray|     AB|
|     2|Peter Smith|     CD|
|     1|   Mark Ray|     EF|
|     2|Peter Smith|     GH|
|     2|Peter Smith|     CD|
|     3|       Kate|     IJ|
+------+-----------+-------+

expected
+------+-----------+--------+
|custid|   custname| address|
+------+-----------+--------+
|     1|   Mark Ray|[EF, AB]|
|     2|Peter Smith|[CD, GH]|
|     3|       Kate|    [IJ]|
+------+-----------+--------+

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

data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]
myschema = ["custid", "custname", "address"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

# DSL
print("SPARK DSL")

df.drop_duplicates().groupby(col("custid"),col("custname")).agg(collect_set(col("address")).alias("address")).show(truncate=False)


print("SPARK SQL")

df.createOrReplaceTempView("sqldf")

spark.sql("""
select custid,custname,collect_set(address) address from sqldf group by custid,custname
""").show(truncate=False)