"""
window lag

Write a code to identify employee_IDs whose performance score increased in three consecutive years.

input:

| employee_ID | performance_score | year |
| ----------- | ----------------- | ---- |
| 1           | 85                | 2020 |
| 1           | 80                | 2019 |
| 1           | 75                | 2018 |
| 2           | 70                | 2020 |
| 2           | 70                | 2019 |
| 2           | 65                | 2018 |
| 3           | 72                | 2019 |
| 3           | 68                | 2018 |




imp note:

fetch the previous two values into a row and compare

"""
# print(__doc__)

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



from pyspark.sql import SparkSession

from pyspark.sql.functions import  *

spark = SparkSession.builder.appName("StudentData").getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql import  Window

spark = SparkSession.builder.appName("EmployeePerformance").getOrCreate()

data = [
    (1, 85, 2020),
    (1, 80, 2019),
    (1, 75, 2018),
    (2, 70, 2020),
    (2, 70, 2019),
    (2, 65, 2018),
    (3, 72, 2019),
    (3, 68, 2018)
]

columns = ["employee_ID", "performance_score", "year"]

df = spark.createDataFrame(data, columns)

df.show()

window_spec = Window.partitionBy("employee_ID").orderBy("year")

df_with_lag = df \
    .withColumn("prev1", lag("performance_score", 1).over(window_spec)) \
    .withColumn("prev2", lag("performance_score", 2).over(window_spec))

df_with_lag.show()

dffltr=df_with_lag.filter((col("prev2").isNotNull()) & (col("prev2")<col("prev1"))& (col("prev1")<col("performance_score")))

dffltr.show()