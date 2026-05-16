"""
input :
+-----+----------------+
|Sl_No|       Full_Name|
+-----+----------------+
|    1|   Ram-Kumar_Das|
|    2|  Raj-Kumar_Sahu|
|    3|Hari-Hara_Mishra|
+-----+----------------+

 OutPut :
+-----+-----------+
|Sl_No|Middle_Name|
+-----+-----------+
|    1|      Kumar|
|    2|      Kumar|
|    3|       Hara|
+-----+-----------+


imp note:

sql:

SELECT Sl_No,
       split(split(Full_Name, '-')[1], '_')[0] AS Middle_Name
FROM employee_names;



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
from pyspark.sql.functions import *


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
# Step 1 — Create DataFrames
data = [
    (1, "Ram-Kumar_Das"),
    (2, "Raj-Kumar_Sahu"),
    (3, "Hari-Hara_Mishra")
]

df = spark.createDataFrame(data, ["Sl_No", "Full_Name"])

df.show()
# Extract middle name using split
df_middle = df.withColumn("Middle_Name", split(split(col("Full_Name"), "-")[1], "_")[0]) \
    .select("Sl_No", "Middle_Name")

df_middle.show()

