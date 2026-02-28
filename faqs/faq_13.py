"""
find the min age and max age without using min and max functions

input:



output:




imp note:


"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql import  Window
data = [(1, 23), (2, 45), (3, 12), (4, 67), (5, 34), (6, 18)]
columns = ["id", "age"]
spark = SparkSession.builder.appName("DenseRankMinMax").getOrCreate()
df = spark.createDataFrame(data, columns)
df.show()

window_asc= Window.orderBy(col("age").asc())
window_desc=Window.orderBy(col("age").desc())

df_rank=df.withColumn("asc_rank", dense_rank().over(window_asc)).withColumn("desc_rank", dense_rank().over(window_desc))
fil_asc=df_rank.filter(col("asc_rank")==1).select("age").collect()[0]["age"]
fil_desc=df_rank.filter(col("desc_rank")==1).select("age").collect()[0]["age"]

print("Maximum Age: ",fil_desc)
print("Minimum Age: ",fil_asc)
