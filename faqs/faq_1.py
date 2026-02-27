"""
input:

+------+------+---+----+---+----+
|emp_no|  name|age|dept|sal| mgr|
+------+------+---+----+---+----+
|    10|  Ravi| 30|  it| 98|  60|
|    20|  Raja| 35|  it| 97|  40|
|    30|  Rama| 45|  it| 94|  50|
|    40|Suresh| 55| mgr|200|NULL|
|    50|Mahesh| 58| mgr|201|NULL|
|    60|  Gopi| 59| mgr|205|NULL|
+------+------+---+----+---+----+

output:

+--------------+-------------+------+
| Employee     | Manager     | Sal  |
+--------------+-------------+------+
|Engineer_Ravi |Mgr_Gopi     |Third |
|Engineer_Raja |Mgr_Suresh   |Second|
|Engineer_Rama |Mgr_Mahesh   |First |
+--------------+-------------+------+

SELECT
    'Engineer_' || e.name AS Employee,
    'Mgr_' || m.name      AS Manager,
    CASE DENSE_RANK() OVER (ORDER BY e.sal ASC)
        WHEN 1 THEN 'First'
        WHEN 2 THEN 'Second'
        WHEN 3 THEN 'Third'
    END AS Sal
FROM emp e
JOIN emp m
    ON e.mgr = m.emp_no
WHERE e.dept = 'it'
ORDER BY e.sal DESC;

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


from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql import Window


config=SparkConf().setAppName("AppOne").setMaster("local[*]")
sc= SparkContext(conf=config)
spark=SparkSession.builder.getOrCreate()

data = [
    (10, "Ravi", 30, "it", 98, 60),
    (20, "Raja", 35, "it", 97, 40),
    (30, "Rama", 45, "it", 94, 50),
    (40, "Suresh", 55, "mgr", 200, None),
    (50, "Mahesh", 58, "mgr", 201, None),
    (60, "Gopi", 59, "mgr", 205, None)
]

columns = ["emp_no", "name", "age", "dept", "sal", "mgr"]

df = spark.createDataFrame(data, columns)

df.show()

empdf=df.alias("e")
mngrdf=df.alias("m")

print("joined df")
joindf=empdf.join(mngrdf,(col("e.mgr")== col("m.emp_no")),"inner")\
    .filter("e.dept=='it'").orderBy(col("e.sal").desc())
joindf.show()


# add rank column

window=Window.partitionBy().orderBy(col("e.sal").asc())

rdf=joindf.withColumn("rank",dense_rank().over(window))
rdf.show()



rdf.select(concat(lit("Engineer_"),col("e.name")).alias("employee"), \
           concat(lit("Manger_"),col("m.name")).alias("manager"),\
           when(col("rank")==1,"First"). when(col("rank")==2,"Second")\
           . when(col("rank")==3,"Third").alias("sal")
           ).orderBy(col("sal").desc()).show()



df.createOrReplaceTempView("emp")

# print("SPARK SQL")
# spark.sql("""
# select
# concat('Employee_',e.name) as Employee,
# concat('Manager_',m.name) as Manager,
#  CASE DENSE_RANK() OVER (ORDER BY e.sal ASC)
#         WHEN 1 THEN 'First'
#         WHEN 2 THEN 'Second'
#         WHEN 3 THEN 'Third'
# END AS Sal
# from emp e inner join emp m
# on e.mgr=m.emp_no
# where e.dept='it'
# order by e.sal desc
# """).show()


print("SPARK SQL")
spark.sql("""
select 
concat('Employee_',e.name) as Employee,
concat('Manager_',m.name) as Manager,
 CASE 
        WHEN DENSE_RANK() OVER (ORDER BY e.sal ASC)==1 THEN 'First'
        WHEN DENSE_RANK() OVER (ORDER BY e.sal ASC)==2 THEN 'Second'
        WHEN DENSE_RANK() OVER (ORDER BY e.sal ASC)==3 THEN 'Third'
END AS Sal
from emp e inner join emp m
on e.mgr=m.emp_no
where e.dept='it'
order by e.sal desc
""").show()