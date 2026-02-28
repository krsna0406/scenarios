"""

 Fill nulls with last non-null value above using SQL and Pyspark


SELECT
    Id,
    LAST_VALUE(coursename IGNORE NULLS)
    OVER (
        ORDER BY Id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS coursename
FROM table_name;


input:

+---+----------------+
| Id|      coursename|
+---+----------------+
|  1|            Java|
|  2|            NULL|
|  3|            NULL|
|  4|            NULL|
|  5|Data Engineering|
|  6|            NULL|
+---+----------------+


output:

+---+----------------+----------------+
| Id|      coursename|      new_cource|
+---+----------------+----------------+
|  1|            Java|            Java|
|  2|            NULL|            Java|
|  3|            NULL|            Java|
|  4|            NULL|            Java|
|  5|Data Engineering|Data Engineering|
|  6|            NULL|Data Engineering|
+---+----------------+----------------+



imp note:
This is called Forward Fill (or Carry Forward Last Non-Null Value).

important note always use rows between ,

default is range between (in which we get wrong anwers as it counts the sum etc on some conditions

like id<2 etc... we get wrong values when we get the duplicate ids  check the  document imp_n for more details.())

"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql import  Window

spark = SparkSession.builder.appName("example").getOrCreate()
data = [
    (1, "Java"),
    (2, None),
    (3, None),
    (4, None),
    (5, "Data Engineering"),
    (6, None)
]

df = spark.createDataFrame(data, ["Id", "coursename"])

df.show()

from pyspark.sql import  Window
window= Window.orderBy(col("id")).rowsBetween(Window.unboundedPreceding,Window.currentRow)

df1=df.withColumn("new_cource", last("coursename",ignorenulls=True).over(window))

df1.show()