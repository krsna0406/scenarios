"""

scenario 21 :The roundtrip distance should be calculated using spark or SQL.
input:

+----+---+----+
|from| to|dist|
+----+---+----+
| SEA| SF| 300|
| CHI|SEA|2000|
|  SF|SEA| 300|
| SEA|CHI|2000|
| SEA|LND| 500|
| LND|SEA| 500|
| LND|CHI|1000|
| CHI|NDL| 180|
+----+---+----+

expected :

+----+---+--------------+
|from| to|roundtrip_dist|
+----+---+--------------+
| SEA| SF|           600|
| CHI|SEA|          4000|
| LND|SEA|          1000|
+----+---+--------------+

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
        ("SEA", "SF", 300),
        ("CHI", "SEA", 2000),
        ("SF", "SEA", 300),
        ("SEA", "CHI", 2000),
        ("SEA", "LND", 500),
        ("LND", "SEA", 500),
        ("LND", "CHI", 1000),
        ("CHI", "NDL", 180)]
df = spark.createDataFrame(data, ["from", "to", "dist"])
df.show()

# print("JOINED DATA")
# df.alias("a").join(   df.alias("b"),(  (col("a.from") == col("b.to")) & ( col("a.to") == col("b.from") ) ),"inner")\
#         .show()


# Through SQL
df.createOrReplaceTempView("trip")
spark.sql("""SELECT r1.from, r1.to, (r1.dist + r2.dist) AS roundtrip_dist
FROM trip r1
JOIN trip r2 ON r1.from = r2.to AND r1.to = r2.from
WHERE r1.from < r1.to
""").show()


# Through DSL
finaldf = df.alias("r1").join(df.alias("r2"),
                              (col("r1.from") == col("r2.to")) & (col("r1.to") == col("r2.from"))).where(
        col("r1.from") < col("r1.to")).select(col("r1.from"), col("r1.to"),
                                              (col("r1.dist") + col("r2.dist")).alias("roundtrip_dist"))

finaldf.show()