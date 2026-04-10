"""
Customer table and orders table
1. Find the customer who had order more than 1
2. Customer who didn't, ordered

SQL:
SQL Query

cgpt:
Assume two tables:

Customers
---------
customer_id
customer_name
Orders
------
order_id
customer_id

1. Customers who placed more than 1 order

SELECT c.customer_id, c.customer_name
FROM Customers c
JOIN Orders o
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
HAVING COUNT(o.order_id) > 1;

Logic

Join customers with orders

Count orders per customer

Filter where count > 1

2. Customers who did not place any order
SELECT c.customer_id, c.customer_name
FROM Customers c
LEFT JOIN Orders o
ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

Logic

LEFT JOIN keeps all customers

If customer has no order, order columns become NULL

Example
Customers
customer_id	customer_name
1	Alice
2	Bob
3	Charlie
Orders
order_id	customer_id
101	1
102	1
103	2
Query 1 Output (more than 1 order)
customer_id	customer_name
1	Alice
Query 2 Output (no orders)
customer_id	customer_name
3	Charlie

💡 Interview Tip

Sometimes interviewers also ask:

Customers who ordered exactly once

SELECT c.customer_id, c.customer_name
FROM Customers c
JOIN Orders o
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
HAVING COUNT(o.order_id) = 1;





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
