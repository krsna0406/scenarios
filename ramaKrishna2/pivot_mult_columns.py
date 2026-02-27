"""

scenario :

scenario to do the pivot and grouping of multiple columns
input:
expected :

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

# Aggregate by Player and Stadium
agg_df = df.groupBy("Player", "Stadium").agg(
    sum("Runs").alias("Total_Runs"),
    sum("Wickets").alias("Total_Wickets")
)

agg_df.show()


# Pivot Stadium into columns
pivot_df = agg_df.groupBy("Player").pivot("Stadium").agg(
    sum("Total_Runs").alias("Runs"),
    sum("Total_Wickets").alias("Wickets")
)

pivot_df.show()


# Rename columns for clarity
final_df = pivot_df.selectExpr(
    "Player",
    "`Wankhede_Runs` as Runs_In_Wankhede",
    "`Wankhede_Wickets` as Wickets_In_Wankhede",
    "`Eden_G_Runs` as Runs_In_Eden_G",
    "`Eden_G_Wickets` as Wickets_In_Eden_G"
)

final_df.show()




spark=SparkSession.builder.getOrCreate()


