"""

Delete the duplicate records from a table without using any pre-defined functions.

SQL:
SQL Query
DELETE e1
FROM Employee e1
JOIN Employee e2
ON  e1.name = e2.name
AND e1.dept = e2.dept
AND e1.salary = e2.salary
AND e1.id > e2.id;


cgpt:

To delete duplicate records without using aggregate/window functions (like ROW_NUMBER(), COUNT(), etc.), you typically use a self-join with a unique identifier such as id.

Assume table:

Employee
--------
id
name
dept
salary

Duplicates mean rows having same (name, dept, salary) but different id.

SQL Query
DELETE e1
FROM Employee e1
JOIN Employee e2
ON  e1.name = e2.name
AND e1.dept = e2.dept
AND e1.salary = e2.salary
AND e1.id > e2.id;
Logic

Table is joined with itself.

If two rows have the same data, we compare id.

The row with larger id is considered duplicate.

That row gets deleted.

Example

Table before:

id	name	dept	salary
1	A	    IT	    5000
2	A	    IT	    5000
3	B	    HR	    4000
4	B	    HR	    4000

After query:

id	name	dept	salary
1	A	    IT	    5000
3	B	    HR	    4000

Duplicates removed.

Key Idea

Condition:

e1.id > e2.id

ensures:

Keep first occurrence

Delete later duplicates


extra tips:

Here are 4 common SQL approaches to delete duplicates. Interviewers often expect you to know these patterns.

Assume table:

Employee
--------
id
name
dept
salary

Duplicates = rows having same (name, dept, salary).

1. Self Join (No predefined functions)

Already discussed.

DELETE e1
FROM Employee e1
JOIN Employee e2
ON e1.name = e2.name
AND e1.dept = e2.dept
AND e1.salary = e2.salary
AND e1.id > e2.id;

Idea

Compare two rows.

Delete the one with higher id.

2. Using ROW_NUMBER() (Most common modern solution)
DELETE FROM Employee
WHERE id IN (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER(
               PARTITION BY name, dept, salary
               ORDER BY id) AS rn
        FROM Employee
    ) t
    WHERE rn > 1
);

Logic

ROW_NUMBER() assigns ranking inside duplicates.

Keep rn = 1.

Delete rn > 1.

Example ranking:

id	name	dept	salary	rn
1	A	IT	5000	1
2	A	IT	5000	2

Row 2 gets deleted.

3. Using GROUP BY + MIN(id)
DELETE FROM Employee
WHERE id NOT IN (
    SELECT MIN(id)
    FROM Employee
    GROUP BY name, dept, salary
);

Logic

For each duplicate group keep minimum id.

Delete all others.

4. Using Temporary Table

Step 1: create clean table

CREATE TABLE temp_employee AS
SELECT DISTINCT *
FROM Employee;

Step 2:

TRUNCATE TABLE Employee;

Step 3:

INSERT INTO Employee
SELECT * FROM temp_employee;

Idea

DISTINCT removes duplicates.

Interview Tip

Interviewers sometimes ask follow-up questions:

Q1: What if there is no primary key / id column?

Use ROW_NUMBER():

SELECT *
FROM (
    SELECT *,
    ROW_NUMBER() OVER(
    PARTITION BY name, dept, salary) rn
    FROM Employee
) t
WHERE rn = 1;
Q2: How to find duplicates only?
SELECT name, dept, salary, COUNT(*)
FROM Employee
GROUP BY name, dept, salary
HAVING COUNT(*) > 1;



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
