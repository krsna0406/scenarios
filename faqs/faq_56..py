"""

gave a table which has joining date and relieving date.
Asked me to find the duration between these two dates

input:

+------+----+------------+--------------+
|emp_id|name|joining_date|relieving_date|
+------+----+------------+--------------+
|     1|Ravi|  2020-01-10|    2023-03-15|
|     2|Raja|  2019-05-01|    2021-06-20|
|     3|Rama|  2022-02-01|    2024-01-01|
+------+----+------------+--------------+

output:



+------+----+------------+--------------+-------------+
|emp_id|name|joining_date|relieving_date|duration_days|
+------+----+------------+--------------+-------------+
|     1|Ravi|  2020-01-10|    2023-03-15|         1160|
|     2|Raja|  2019-05-01|    2021-06-20|          781|
|     3|Rama|  2022-02-01|    2024-01-01|          699|
+------+----+------------+--------------+-------------+

+------+----+------------+--------------+---------------+
|emp_id|name|joining_date|relieving_date|duration_months|
+------+----+------------+--------------+---------------+
|     1|Ravi|  2020-01-10|    2023-03-15|    38.16129032|
|     2|Raja|  2019-05-01|    2021-06-20|    25.61290323|
|     3|Rama|  2022-02-01|    2024-01-01|           23.0|
+------+----+------------+--------------+---------------+

+------+----+------------+--------------+--------------+
|emp_id|name|joining_date|relieving_date|duration_years|
+------+----+------------+--------------+--------------+
|     1|Ravi|  2020-01-10|    2023-03-15|          3.18|
|     2|Raja|  2019-05-01|    2021-06-20|          2.13|
|     3|Rama|  2022-02-01|    2024-01-01|          1.92|
+------+----+------------+--------------+--------------+

SQL:


note:

gpt:

simply need to compute the difference between joining_date and relieving_date.

Below are typical PySpark and SQL approaches used in interviews.

1️⃣ Example Table
emp_id	joining_date	relieving_date
1	    2020-01-10	    2023-03-15
2	    2019-05-01	    2021-06-20

2️⃣ PySpark Solution

Create Sample Data
from pyspark.sql.functions import *

data = [ (1,"2020-01-10","2023-03-15"), (2,"2019-05-01","2021-06-20") ]
cols = ["emp_id","joining_date","relieving_date"]
df = spark.createDataFrame(data, cols)

Convert to date type

df = df.withColumn("joining_date", to_date("joining_date")) \
       .withColumn("relieving_date", to_date("relieving_date"))
Duration in Days
df.withColumn(
    "duration_days",
    datediff("relieving_date","joining_date")
).show()

Result

emp_id	duration_days
1	1160
2	781
Duration in Months
df.withColumn(
    "duration_months",
    months_between("relieving_date","joining_date")
)
Duration in Years
df.withColumn(
    "duration_years",
    months_between("relieving_date","joining_date")/12
)
3️⃣ SQL Solution
SELECT
emp_id,
DATEDIFF(relieving_date, joining_date) AS duration_days
FROM employee;
4️⃣ Interview Trick (Years + Months + Days)

Sometimes interviewers want exact tenure.

df.withColumn(
    "tenure_years",
    floor(months_between("relieving_date","joining_date")/12)
)
5️⃣ Example Output (Years)
emp_id	tenure_years
1	3
2	2

✅ Concepts Tested

datediff()

months_between()

date type casting

tenure calculations



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
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("EmployeeBonus").getOrCreate()

data = [
    (1,"Ravi","2020-01-10","2023-03-15"),
    (2,"Raja","2019-05-01","2021-06-20"),
    (3,"Rama","2022-02-01","2024-01-01")
]

cols = ["emp_id","name","joining_date","relieving_date"]

df = spark.createDataFrame(data, cols)

df.show()

print("schema before casting---")
df.printSchema()
print("schema after casting---")
# 2️⃣ Convert String → Date
df = df.withColumn("joining_date", to_date("joining_date")) \
    .withColumn("relieving_date", to_date("relieving_date"))

df.printSchema()


# 3️⃣ Duration in Days

print(" Duration in Days > datediff ")
df.withColumn(
    "duration_days",
    datediff("relieving_date","joining_date")
).show()

# Output
#
# +------+----+------------+--------------+-------------+
# |emp_id|name|joining_date|relieving_date|duration_days|
# +------+----+------------+--------------+-------------+
# |1     |Ravi|2020-01-10  |2023-03-15    |1160         |
# |2     |Raja|2019-05-01  |2021-06-20    |781          |
# |3     |Rama|2022-02-01  |2024-01-01    |699          |
# +------+----+------------+--------------+-------------+
# 4️⃣ Duration in Months
print(" Duration in Months > months_between ")
df.withColumn(
    "duration_months",
    months_between("relieving_date","joining_date")
).show()
# 5️⃣ Duration in Years

print(" duration_years > months_between /12  ")


df.withColumn(
    "duration_years",
    round(months_between("relieving_date","joining_date")/12,2)
).show()
#
# Example Output
#
# +------+----+------------+--------------+--------------+
# |emp_id|name|joining_date|relieving_date|duration_years|
# +------+----+------------+--------------+--------------+
# |1     |Ravi|2020-01-10  |2023-03-15    |3.18          |
# |2     |Raja|2019-05-01  |2021-06-20    |2.14          |
# |3     |Rama|2022-02-01  |2024-01-01    |1.92          |
# +------+----+------------+--------------+--------------+
# 6️⃣ One-Line Interview Solution
#
# Many interviewers expect this short solution:

df.withColumn("duration_days",
              datediff(col("relieving_date"), col("joining_date")))