"""
spark program to calculate the count of employee skills,
sum of bonus and average of bonus for each employee

input:
empname	emp_skill	bonus
a	Java	20
a	Hql	30
a	Spark	50
b	MS	10
b	Azure	40

output:

Output:
empname	skill_count	total_bonus	avg_bonus
a	3	100	33.33
b	2	50	25.00





note:



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
from pyspark.sql.functions import count, sum, avg, round
spark = SparkSession.builder.appName("EmployeeBonus").getOrCreate()
data = [
    ("a", "Java", 20),
    ("a", "Hql", 30),
    ("a", "Spark", 50),
    ("b", "MS", 10),
    ("b", "Azure", 40)
]
columns = ["empname", "emp_skill", "bonus"]
df = spark.createDataFrame(data, columns)
result_df = (
    df.groupBy("empname")
    .agg(
        count("emp_skill").alias("skill_count"),
        sum("bonus").alias("total_bonus"),
        round(avg("bonus"), 2).alias("avg_bonus")
    )
)
result_df.show()
