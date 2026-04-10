"""
Table of students
Col - Roll No, Name
Swap the names of the Adjacent Students


SQL:
SQL Query

cgpt:
Assume table:

Students
---------
roll_no
name

Goal: Swap names of adjacent students
(1↔2, 3↔4, 5↔6 …)

Example

roll_no	name
1	A
2	B
3	C
4	D
5	E

Expected Output

roll_no	name
1	B
2	A
3	D
4	C
5	E

(Last row remains same if odd count)

SQL Solution (Using CASE)
SELECT
    s1.roll_no,
    CASE
        WHEN s1.roll_no % 2 = 1 THEN s2.name
        WHEN s1.roll_no % 2 = 0 THEN s3.name
        ELSE s1.name
    END AS name
FROM Students s1
LEFT JOIN Students s2
    ON s1.roll_no + 1 = s2.roll_no
LEFT JOIN Students s3
    ON s1.roll_no - 1 = s3.roll_no
ORDER BY s1.roll_no;
Logic

If roll number is odd, take next student's name.

If roll number is even, take previous student's name.

LEFT JOIN handles the last odd row safely.

Simpler Interview Solution

Often interviewers accept this:

SELECT
CASE
    WHEN roll_no % 2 = 1
         AND roll_no != (SELECT MAX(roll_no) FROM Students)
         THEN roll_no + 1
    WHEN roll_no % 2 = 0 THEN roll_no - 1
    ELSE roll_no
END AS roll_no,
name
FROM Students
ORDER BY roll_no;


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

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()


data = [
    (1, "Arjun"),
    (2, "Bhanu"),
    (3, "Charan"),
    (4, "Deepak"),
    (5, "Esha"),
    (6, "Farhan"),
    (7, "Gita")
]

columns = ["roll_no", "name"]

df = spark.createDataFrame(data, columns)
df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead, lag, when

w = Window.orderBy("roll_no")

df2 = df.withColumn(
    "swapped_name",
    when(col("roll_no") % 2 == 1, lead("name").over(w))
    .otherwise(lag("name").over(w))
)

df2 = df2.withColumn(
    "final_name",
    when(col("swapped_name").isNull(), col("name"))
    .otherwise(col("swapped_name"))
)

df2.select("roll_no", "final_name").show()

# Shorter PySpark Interview Solution

df.withColumn(
    "name",
    when(col("roll_no") % 2 == 1, lead("name").over(w))
    .otherwise(lag("name").over(w))
).na.fill({"name": "Gita"}).show()