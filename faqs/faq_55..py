"""


File = input.txt
data:
name.age.sal
name.age.sal
#Ravi.b35.$98
Raja@.3~5.98*


Delta Table Output

Name	Age	Sal	Salary_Rank
Ravi	35	98	1
Raja	35	98	1

input:

output:

SQL:


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

# 1️⃣ Read Text File
df = spark.read.text("input.txt")
df.show(truncate=False)

# Output
#
# +--------------+
# |value         |
# +--------------+
# |#Ravi.b35.$98 |
# |Raja@.3~5.98* |
# +--------------+

# 2️⃣ Extract Clean Columns (Regex)
#
# Use regexp_extract().

from pyspark.sql.functions import *

clean_df = df.select(
    regexp_extract("value", "[A-Za-z]+", 0).alias("Name"),
    regexp_extract("value", "(\\d{2})", 0).cast("int").alias("Age"),
    regexp_extract("value", "(\\d{2})$", 0).cast("int").alias("Sal")
)

clean_df.show()

# Output
#
# +----+---+---+
# |Name|Age|Sal|
# +----+---+---+
# |Ravi|35 |98 |
# |Raja|35 |98 |
# +----+---+---+

# 3️⃣ Salary Rank
#
# Use dense_rank().

from pyspark.sql.window import Window

w = Window.orderBy(col("Sal").desc())

final_df = clean_df.withColumn(
    "Salary_Rank",
    dense_rank().over(w)
)

final_df.show()
#
# Output
#
# +----+---+---+-----------+
# |Name|Age|Sal|Salary_Rank|
# +----+---+---+-----------+
# |Ravi|35 |98 |1|
# |Raja|35 |98 |1|
# +----+---+---+-----------+
# 4️⃣ Write to Delta Table
final_df.write.format("delta") \
    .mode("overwrite") \
    .save("/delta/employee_salary")

# Or register table

final_df.write.format("delta").saveAsTable("employee_salary")