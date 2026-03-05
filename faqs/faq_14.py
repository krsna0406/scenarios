"""


input:
+-------+--------------------------+
|User_id|email                     |
+-------+--------------------------+
|U1     |john.wick@gmail.com       |
|U2     |babu.bhaiya@stargarage.org|
|U3     |anuradha@laxmichitfund    |
|U4     |raju@.ius                 |
+-------+--------------------------+


output:

+-------+--------------------------+--------------+
|User_id|email                     |company_domain|
+-------+--------------------------+--------------+
|U1     |john.wick@gmail.com       |gmail.com     |
|U2     |babu.bhaiya@stargarage.org|stargarage.org|
|U3     |anuradha@laxmichitfund    |NULL          |
|U4     |raju@.ius                 |NULL          |
+-------+--------------------------+--------------+


imp note:

This is an email domain extraction + validation problem.

Rules from expected output:

Extract domain part after @

Domain must:

Contain at least one .
Have characters before and after .
Otherwise → NULL


regex :

🔍 Breakdown Character by Character
1️⃣ ^

Start of string.

Ensures the domain begins exactly here.
No characters allowed before it.

2️⃣ [A-Za-z0-9-]+

Character class:

A-Z → uppercase letters

a-z → lowercase letters

0-9 → digits

- → hyphen

+ → one or more occurrences

This represents the main domain name

Examples matched:

gmail
stargarage
abc123
company-name

Not matched:

.gmail
@domain
3️⃣ \.

Literal dot.

Important:

. alone in regex means "any character".

So we escape it:

\.  → actual dot

In PySpark string → must write \\.

4️⃣ [A-Za-z]{2,}

Top Level Domain (TLD)

Letters only

Minimum length 2

Matches:

com
org
in
co
net

Does NOT match:

c
123
.
5️⃣ $

End of string.

Ensures nothing exists after TLD.


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
spark = SparkSession.builder.appName("ex").getOrCreate()

data = [
    ("U1", "john.wick@gmail.com"),
    ("U2", "babu.bhaiya@stargarage.org"),
    ("U3", "anuradha@laxmichitfund"),
    ("U4", "raju@.ius")
]

columns = ["User_id", "email"]

df = spark.createDataFrame(data, columns)

df.show(truncate=False)


df_result = df.withColumn(
    "domain_part",
    split(col("email"), "@").getItem(1)
).withColumn(
    "company_domain",
    when(
        col("domain_part").rlike("^[A-Za-z0-9-]+\\.[A-Za-z]{2,}$"),
        col("domain_part")
    ).otherwise(None)
).drop("domain_part")

df_result.show(truncate=False)


