"""
Delete the latest entry of Duplicates in Employees table(unique records per an Employee)

input:
+---+------+----+------+
| id|emp_id|name|salary|
+---+------+----+------+
|  1|   101|   A|  5000|
|  2|   101|   A|  5000|
|  3|   102|   B|  6000|
|  4|   102|   B|  6000|
|  5|   103|   C|  7000|
+---+------+----+------+
output:

+---+------+----+------+
| id|emp_id|name|salary|
+---+------+----+------+
|  1|   101|   A|  5000|
|  3|   102|   B|  6000|
|  5|   103|   C|  7000|
+---+------+----+------+

imp note:

Assume table Employees

emp_id | emp_name | dept | salary

Duplicates exist for the same employee, and we want to delete the latest duplicate entry, keeping only one unique record per employee.

Usually we identify the latest entry using a column like id or created_at.

1️⃣ Example Data
id	emp_id	name	salary
1	101	A	5000
2	101	A	5000
3	102	B	6000
4	102	B	6000
5	103	C	7000

Here:

emp_id 101 → duplicate
emp_id 102 → duplicate

We must delete the latest duplicate (highest id).

2️⃣ SQL Solution (Using ROW_NUMBER)
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY emp_id ORDER BY id DESC) AS rn
    FROM employees
)
DELETE FROM employees
WHERE id IN (
    SELECT id
    FROM cte
    WHERE rn = 1
);  HERE NEED TO GET THE COUNT TO COMPARE


SQL:

"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

from pyspark.sql import SparkSession


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
data = [
    (1,101,"A",5000),
    (2,101,"A",5000),
    (3,102,"B",6000),
    (4,102,"B",6000),
    (5,103,"C",7000)
]

cols = ["id","emp_id","name","salary"]
df = spark.createDataFrame(data, cols)

df.show()

#2) Rank Rows Per Employee (latest first)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("emp_id").orderBy(col("id").desc())

df_ranked = df.withColumn("rn", row_number().over(w))
df_ranked.show()

#3) Delete Latest Duplicate
from pyspark.sql.functions import count

w2 = Window.partitionBy("emp_id")

result = (
    df_ranked
    .withColumn("cnt", count("*").over(w2))
    .filter(~((col("rn") == 1) & (col("cnt") > 1)))  # remove latest duplicate
    .drop("rn","cnt")
)

result.show()


