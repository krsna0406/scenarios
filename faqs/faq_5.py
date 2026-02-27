"""
without pivot ..conditional aggregations VVVIMP


input:

+---------+---------+-----+----+
|StudentId|  Subject|Marks|Year|
+---------+---------+-----+----+
|        1|  English|   90|2024|
|        1|    Maths|   91|2024|
|        1|  Physics|   92|2024|
|        1|Chemistry|   93|2024|
|        1|  Physics|   98|2025|
|        1|Chemistry|   87|2025|
|        1|  English|   92|2025|
|        1|    Maths|   95|2025|
|        1|  Biology|   70|2024|
+---------+---------+-----+----+

output:

| StudentId | English | Maths | Physics | Chemistry | Biology | Year |
| --------- | ------- | ----- | ------- | --------- | ------- | ---- |
| 1         | 90      | 91    | 92      | 93        | 70      | 2024 |
| 1         | 92      | 95    | 98      | 87        | 0       | 2025 |


imp note:

2️⃣ pivot() — ⚠ Important

pivot() expects a column name string, not a Column expression.

Correct usage per Spark API:

pivot("Subject")

When you write: IMPPPPPPPPPPPP  dont use col in pivot

pivot(col("Subject"))
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

data = [
    (1, "English", 90, 2024),
    (1, "Maths", 91, 2024),
    (1, "Physics", 92, 2024),
    (1, "Chemistry", 93, 2024),
    (1, "Physics", 98, 2025),
    (1, "Chemistry", 87, 2025),
    (1, "English", 92, 2025),
    (1, "Maths", 95, 2025),
    (1, "Biology", 70, 2024)
]

columns = ["StudentId", "Subject", "Marks", "Year"]

df = spark.createDataFrame(data, columns)

df.show()

# by using spark sql
print("SPARK SQL")
df.createOrReplaceTempView("student")


spark.sql("""
select StudentId,Year,
COALESCE(max(case when Subject='English' then 'marks' else 0 end ),0) as english,
COALESCE(max(case when Subject='Maths' then 'marks' else 0 end ),0) as Maths,
COALESCE(max(case when Subject='Physics' then 'marks' else 0 end ),0) as Physics,
COALESCE(max(case when Subject='Chemistry' then 'marks' else 0 end ),0) as Chemistry,
COALESCE(max(case when Subject='Biology' then 'marks' else 0 end ),0)  as Biology
from student
group by StudentId,Year
""").show()

print("SPARK DSL")


df.groupby(col("StudentId"),col("Year"))\
    .agg(coalesce( max( when(col("Subject")=='English' ,col('marks')) ) ,lit(0)).alias("English"), \
         coalesce( max( when(col("Subject")=='Maths' ,col('marks')) ) ,lit(0)).alias("Maths"), \
         coalesce( max( when(col("Subject")=='Physics' ,col('marks')) ) ,lit(0)).alias("Physics"),\
         coalesce( max( when(col("Subject")=='Chemistry' ,col('marks')) ) ,lit(0)).alias("Chemistry"), \
         coalesce( max( when(col("Subject")=='Biology' ,col('marks')) ) ,lit(0)).alias("Biology"),
         ).show()

print("by using pivot")
#GPS

df.groupby(col("StudentId"),col("Year"))\
    .pivot("Subject"). agg(sum(col("Marks"))) \
    .na.fill(0).show()


# df.groupBy("StudentId", "Year") \
#     .pivot("Subject") \
#     .agg(sum("Marks")) \
#     .na.fill(0).show()

