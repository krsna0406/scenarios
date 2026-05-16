"""

input:

+------+-------+---+----+---+----+
|emp_no|name   |age|dept|sal|mgr |
+------+-------+---+----+---+----+
|10    |Ravi   |30 |it  |98 |60  |
|20    |Raja   |35 |it  |97 |40  |
|30    |Rama   |45 |it  |94 |50  |
|40    |Suresh |55 |mgr |200|null|
|50    |Mahesh |58 |mgr |201|null|
|60    |Gopi   |59 |mgr |205|null|
+------+-------+---+----+---+----+




output:

+--------------+-----------+------+
|Employee      |Manager    |Sal   |
+--------------+-----------+------+
|Engineer_Ravi |Mgr_Gopi   |Third |
|Engineer_Raja |Mgr_Suresh |Second|
|Engineer_Rama |Mgr_Mahesh |First |
+--------------+-----------+------+


SQL:


WITH emp_mgr AS (
    SELECT
        e.name AS emp_name,
        m.name AS mgr_name,
        e.sal,
        ROW_NUMBER() OVER(ORDER BY e.sal) AS rn
    FROM employee e
    LEFT JOIN employee m
        ON e.mgr = m.emp_no
    WHERE e.dept = 'it'
)

SELECT
    CONCAT('Engineer_', emp_name) AS Employee,
    CONCAT('Mgr_', mgr_name) AS Manager,
    CASE rn
        WHEN 1 THEN 'First'
        WHEN 2 THEN 'Second'
        WHEN 3 THEN 'Third'
    END AS Sal
FROM emp_mgr
ORDER BY sal DESC;



note:
Interpretation of your scenario:

Table contains employees and managers in the same table.

mgr column → points to manager Emp.No.

Need to:

Join employee with manager name.

Prefix names:

Employee → Engineer_

Manager → Mgr_

Rank engineers by salary (ascending) and map to
First, Second, Third.

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
    (10,"Ravi",30,"it",98,60),
    (20,"Raja",35,"it",97,40),
    (30,"Rama",45,"it",94,50),
    (40,"Suresh",55,"mgr",200,None),
    (50,"Mahesh",58,"mgr",201,None),
    (60,"Gopi",59,"mgr",205,None)
]

cols = ["emp_no","name","age","dept","sal","mgr"]

df = spark.createDataFrame(data, cols)

df.show()

from pyspark.sql import  Window
from pyspark.sql.functions import  *

emp = df.alias("e")
mgr = df.alias("m")

# self join
joined = emp.join(mgr, col("e.mgr")==col("m.emp_no"), "left")

# window for salary ranking
w = Window.orderBy("e.sal")

result = joined.filter(col("e.dept")=="it") \
    .withColumn("rn", row_number().over(w)) \
    .withColumn("Employee", concat(lit("Engineer_"), col("e.name"))) \
    .withColumn("Manager", concat(lit("Mgr_"), col("m.name"))) \
    .withColumn("salary",
                when(col("rn")==1,"First")
                .when(col("rn")==2,"Second")
                .when(col("rn")==3,"Third")
                ) \
     .select("Employee","Manager","salary") \
     .orderBy(col("rn").desc())

result.show()

