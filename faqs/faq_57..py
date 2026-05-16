"""

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
from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

data = [
    (1, 'A', 1, 100),
    (1, 'A', 2, 200),
    (1, 'A', 3, 300),
    (1, 'A', 4, 400),
    (1, 'A', 5, 500),
    (2, 'B', 1, 50),
    (2, 'B', 2, 150),
    (2, 'B', 3, 250),
]

columns = ["emp_id", "item_no", "week", "salary"]

df = spark.createDataFrame(data, columns)

df.show()

window_2w = Window.partitionBy("emp_id", "item_no") \
    .orderBy("week") \
    .rowsBetween(-1, 0)

window_4w = Window.partitionBy("emp_id", "item_no") \
    .orderBy("week") \
    .rowsBetween(-3, 0)

df_result = df.withColumn("salary_2w", sum("salary").over(window_2w)) \
    .withColumn("salary_4w", sum("salary").over(window_4w))

df_result.show()