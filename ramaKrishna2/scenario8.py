"""

scenario 8:

INPUT:

+--------+
|   teams|
+--------+
|   India|
|Pakistan|
|SriLanka|
+--------+

OUTPUT:
+--------------------+
|             matches|
+--------------------+
|   India Vs Pakistan|
|   India Vs SriLanka|
|Pakistan Vs SriLanka|
+--------------------+

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

data = [
    ("India",),
    ("Pakistan",),
    ("SriLanka",)
]
myschema = ["teams"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

df_c=df.toDF("teams1")

df_c.printSchema()

df1=df.alias("a").join(df_c.alias("b"),col("a.teams")<col("b.teams1"),"inner")


print("MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM")

# df1.withColumn("matches",concat(col("teams"), lit(" Vs ") ,col("teams1"))).show()

df1.withColumn("matches",expr("""concat(teams,' Vs ',teams1)""")).show()


# # Through SQL
# df.createOrReplaceTempView("crickettab")
#
# # self join query for reference - select a.teams,b.teams from crickettab a inner join crickettab b on a.teams < b.teams
#
# spark.sql(
#     "select concat(a.teams, '  Vs  ', b.teams) as matches from crickettab a inner join crickettab b on a.teams < b.teams").show()
