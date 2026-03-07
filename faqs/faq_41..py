"""
*customers table*
cust_id     name
1                 A
2                 B
3                 C
4                 D

Orders table
orderid     cust_id
101              1
102              2
103              2

Find customer who have not placed any order in SQL

input:


note:
SQL:
1️⃣ Using LEFT JOIN (Most Common)
SELECT c.cust_id, c.name
FROM customers c
LEFT JOIN orders o
ON c.cust_id = o.cust_id
WHERE o.cust_id IS NULL;

Explanation

LEFT JOIN keeps all customers

Customers without orders → NULL in orders table

Result

cust_id	name
3	C
4	D
2️⃣ Using NOT EXISTS (Best for interviews)
SELECT *
FROM customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM orders o
    WHERE c.cust_id = o.cust_id
);
3️⃣ Using NOT IN
SELECT *
FROM customers
WHERE cust_id NOT IN (
    SELECT cust_id FROM orders
);

⚠️ Note: NOT IN may fail if NULL exists in subquery.


LEFT ANTI

2️⃣ Spark SQL Version
SELECT *
FROM customers c
LEFT ANTI JOIN orders o
ON c.cust_id = o.cust_id;

may not be available in some DBS but ,it works in pyspark.


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
from pyspark.sql.functions import *

from pyspark.sql import Window


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()
# Step 1 — Create DataFrames


customers = [
    (1,"A"),
    (2,"B"),
    (3,"C"),
    (4,"D")
]

orders = [
    (101,1),
    (102,2),
    (103,2)
]

df_customers = spark.createDataFrame(customers,["cust_id","name"])
df_orders = spark.createDataFrame(orders,["orderid","cust_id"])

df_customers.show()
df_orders.show()

result = df_customers.join(df_orders, "cust_id", "left_anti")

result.show()


