"""
identify duplicates in the following data using pyspark
[
  {"id":1, "name":"A"},
  {"id":2, "name":"B"},
  {"id":1, "name":"A"}
]


input:


output:

SQL:

SELECT id, name, COUNT(*) AS cnt
FROM employees
GROUP BY id, name
HAVING COUNT(*) > 1;

3. Show Actual Duplicate Rows
SELECT *
FROM employees
WHERE (id, name) IN (
    SELECT id, name
    FROM employees
    GROUP BY id, name
    HAVING COUNT(*) > 1
);
4. Using Window Function
SELECT id, name
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY id, name) AS rn
    FROM employees
) t
WHERE rn > 1;

This returns duplicate rows only.

5. Quick Interview Trick
SELECT *
FROM employees
GROUP BY id, name
HAVING COUNT(*) > 1;

This identifies which rows are duplicated.

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
    (1, "A"),
    (2, "B"),
    (1, "A")
]

cols = ["id", "name"]

df = spark.createDataFrame(data, cols)

df.show()


# 2. Identify Duplicate Records
# Method 1 — Using groupBy
from pyspark.sql.functions import count

duplicates = df.groupBy("id","name") \
    .agg(count("*").alias("cnt")) \
    .filter("cnt > 1")

duplicates.show()


#IMP 3. Show the Actual Duplicate Rows
duplicate_rows = df.join(
    duplicates.select("id","name"),
    ["id","name"],
    "inner"
)

duplicate_rows.show()



# 4. Shorter Interview Solution
df.groupBy(df.columns).count().filter("count > 1").show()


# 5. Alternative Method (Using dropDuplicates)
# To find duplicates indirectly:

duplicates = df.subtract(df.dropDuplicates())
duplicates.show()

# with windows function

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("id","name")

df.withColumn("rn", row_number().over(w)).filter("rn > 1").show()

