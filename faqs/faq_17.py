"""


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
spark = SparkSession.builder.appName("ex").getOrCreate()
data = [(1, "U1", 100),

              (2, "U1", 200),

              (3, "U1", 100),

              (4, "U2", 150),

              (5, "U2", 150),

              (6, "U3", 300)]

columns = ["txn_id", "user_id", "amount"]

df=spark.createDataFrame(data,columns)
df.show(truncate=False)
# Step 1: Find duplicate (user_id, amount)
dup = (
    df.groupBy("user_id", "amount")
    .agg(count("*").alias("cnt"))
    .filter(col("cnt") > 1)
)

# Step 2: Extract distinct users
result = dup.select("user_id").distinct()

result.show()
