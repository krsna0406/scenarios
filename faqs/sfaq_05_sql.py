"""

You have a table called Teams containing team names.
Write an SQL query to list all unique pairs of teams
that can face each other such that:

SQL:
SQL Query
SELECT
    t1.team_name AS team1,
    t2.team_name AS team2
FROM Teams t1
JOIN Teams t2
    ON t1.team_name < t2.team_name;


cgpt:
To generate all unique pairs of teams that can face each other,
you typically use a self join on the same table while preventing duplicate
and mirror pairs.

Assume table:
Teams
-----
team_name

SQL Query
SELECT
    t1.team_name AS team1,
    t2.team_name AS team2
FROM Teams t1
JOIN Teams t2
    ON t1.team_name < t2.team_name;

Explanation

This is a self join of the Teams table.

t1.team_name < t2.team_name ensures:

A team does not play against itself.

Avoids duplicate mirror pairs like:

(A,B)

(B,A)

So only one unique pair appears.

Example

If Teams contains:

team_name
A
B
C

Output:

team1	team2
A	B
A	C
B	C
Why < instead of != ?

If you write:

t1.team_name != t2.team_name

Output becomes:

team1	team2
A	B
B	A
A	C
C	A
B	C
C	B

So duplicates appear.

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
