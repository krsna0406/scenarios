"""

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

# conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
# sc=SparkContext(conf=conf)

spark=SparkSession.builder\
    .config("spark.sql.shuffle.partitions",3)\
    .config("spark.sql.adaptive.enabled","false")\
    .getOrCreate()

# partition size taken 3 as we have 3 ids in example  ,so one partition gets more records
# and hash(id) % 3   goes in two respective partition  formula >> >>>partition_id=hash(key)mod N



# Table 1 (high skew on id = 1)
id_values1 = [1] * 10000000 + [2] * 5 + [3] * 5
table1 = spark.createDataFrame(id_values1, "int").toDF("id")

# Table 2 (smaller dataset)
id_values2 = [1] * 100 + [2] * 5 + [3] * 2
table2 = spark.createDataFrame(id_values2, "int").toDF("id")
# Optional repartition (commented in your image)
# table1 = table1.repartition(3)
# table2 = table2.repartition(3)

# Join
data = table1.join(table2, on=["id"], how="inner")

# Trigger execution
print(data.count())

# data.show()
df_with_random=table1.withColumn("random_num", (rand()*10+1).cast("int"))

table1= df_with_random.withColumn("salted_key", concat( expr("id"), lit("_"), expr("random_num")))\
    .drop("random_num")

df_with_replicated= table2.withColumn("sequence", array([ lit(i) for i in range(1,11)]))

table2= df_with_replicated\
    .withColumn("exploded_col", explode(col("sequence")))\
    .withColumn("salted_key", concat( expr("id"), lit("_"), expr("exploded_col")    ))\
    .drop("exploded_col","sequence")

#
#
#
#
# print("table1   ")
# table1.show()
# print("table2   ")
#
# table2.show()


# Join
data = table1.join(table2, on=["salted_key"], how="inner")

# Trigger execution
print(data.count())

input("enter any to key to exit ")
