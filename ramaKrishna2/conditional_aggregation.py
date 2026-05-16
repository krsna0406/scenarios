"""

scenario :

scenario to do the pivot and grouping of multiple columns
input:

+------+--------+----+-------+
|Player| Stadium|Runs|Wickets|
+------+--------+----+-------+
|Hardik|Wankhede|  40|      2|
|Hardik|Wankhede|  20|      1|
|Hardik|  Eden_G|  50|      0|
|Jadeja|Wankhede|  70|      1|
|Jadeja|  Eden_G|  30|      1|
|Jadeja|  Eden_G|  20|      1|
+------+--------+----+-------+

expected :
output:

+------+-----------+--------------+-------------+----------------+
|Player|Eden_G_Runs|Eden_G_Wickets|Wankhede_Runs|Wankhede_Wickets|
+------+-----------+--------------+-------------+----------------+
|Jadeja|         50|             2|           70|               1|
|Hardik|         50|             0|           60|               3|
+------+-----------+--------------+-------------+----------------+



sql:

SELECT
    Player,

    SUM(CASE WHEN Stadium = 'Wankhede' THEN Runs ELSE 0 END) AS Runs_In_Wankhede,
    SUM(CASE WHEN Stadium = 'Wankhede' THEN Wickets ELSE 0 END) AS Wickets_In_Wankhede,

    SUM(CASE WHEN Stadium = 'Eden_G' THEN Runs ELSE 0 END) AS Runs_In_Eden_G,
    SUM(CASE WHEN Stadium = 'Eden_G' THEN Wickets ELSE 0 END) AS Wickets_In_Eden_G

FROM your_table
GROUP BY Player;


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


# Sample data
data = [
    ("Hardik", "Wankhede", 40, 2),
    ("Hardik", "Wankhede", 20, 1),
    ("Hardik", "Eden_G",   50, 0),
    ("Jadeja", "Wankhede", 70, 1),
    ("Jadeja", "Eden_G",   30, 1),
    ("Jadeja", "Eden_G",   20, 1),
]

columns = ["Player", "Stadium", "Runs", "Wickets"]

df = spark.createDataFrame(data, columns)

df.show()
#
# # Aggregate by Player and Stadium
# agg_df = df.groupBy("Player", "Stadium").agg(
#     sum("Runs").alias("Total_Runs"),
#     sum("Wickets").alias("Total_Wickets")
# )
#
# agg_df.show()
#
# # Pivot Stadium into columns
# pivot_df = agg_df.groupBy("Player").pivot("Stadium").agg(
#     sum("Total_Runs").alias("Runs"),
#     sum("Total_Wickets").alias("Wickets")
# )
#
# pivot_df.show()
#
#
# # Rename columns for clarity
# final_df = pivot_df.selectExpr(
#     "Player",
#     "`Wankhede_Runs` as Runs_In_Wankhede",
#     "`Wankhede_Wickets` as Wickets_In_Wankhede",
#     "`Eden_G_Runs` as Runs_In_Eden_G",
#     "`Eden_G_Wickets` as Wickets_In_Eden_G"
# )
#
# final_df.show()


# by using spark sql
print("by using spark sql")
df.createOrReplaceTempView("sqldf")
df1=spark.sql(" select * from sqldf")
df1.show(truncate=False)

spark.sql("""
select 
Player,
sum(case when Stadium='Wankhede' then Runs else 0 end) as Wankhede_Runs,
sum(case when Stadium='Wankhede' then Wickets else 0 end) as Wankhede_Wickets,
sum(case when Stadium='Eden_G' then Runs else 0 end) as Eden_G_Runs,
sum(case when Stadium='Eden_G' then Wickets else 0 end) as Eden_G_Wickets
from sqldf 
group by Player 
order by Player desc
""").show()


print("SPARK DSL")

df.groupBy("Player").agg(
    sum( when(col("Stadium")=='Wankhede',col("Runs")).otherwise(0)).alias('Wankhede_Runs'),
    sum( when(col("Stadium")=='Wankhede',col("Wickets")).otherwise(0)).alias('Wankhede_Wickets'),
sum( when(col("Stadium")=='Eden_G',col("Runs")).otherwise(0)).alias('Eden_G_Runs'),
sum( when(col("Stadium")=='Eden_G',col("Wickets")).otherwise(0)).alias('Eden_G_Wickets')
).orderBy(col('Player').desc()).show()