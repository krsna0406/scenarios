"""
INTERVIEW QUESTION
1. how to combine many list

SOLVE USING PYSPARK AND SPARK SQL


notes:

zip() combines elements from multiple iterables position by position.

list1 = [1, 2, 3]
list2 = ['a', 'b', 'c']

result = list(zip(list1, list2))

print(result)

Output:

[(1, 'a'), (2, 'b'), (3, 'c')]

notes:

zip() combines elements from multiple iterables position by position.

list1 = [1, 2, 3]
list2 = ['a', 'b', 'c']

result = list(zip(list1, list2))

print(result)

Output:

[(1, 'a'), (2, 'b'), (3, 'c')]
Step-by-step
1. zip(list1, list2)

Creates a zip object (iterator).

It pairs:

1 with 'a'
2 with 'b'
3 with 'c'

Internally like:

(1, 'a')
(2, 'b')
(3, 'c')

But it is not yet a list.

2. list(...)

Converts the zip iterator into a real list.

So:

list(zip(list1, list2))

becomes:

[(1, 'a'), (2, 'b'), (3, 'c')]
Visual Representation
list1 = [1, 2, 3]
list2 = ['a', 'b', 'c']

        zip
         ↓

1  ----  a   → (1, 'a')
2  ----  b   → (2, 'b')
3  ----  c   → (3, 'c')
Important Behavior
Different Length Lists

zip() stops at the shortest iterable.

list1 = [1, 2, 3, 4]
list2 = ['a', 'b']

print(list(zip(list1, list2)))

Output:

[(1, 'a'), (2, 'b')]

3 and 4 are ignored.

Common Uses
1. Looping two lists together
names = ["Ram", "Sam"]
marks = [90, 80]

for n, m in zip(names, marks):
    print(n, m)

Output:

Ram 90
Sam 80
2. Creating dictionary
keys = ["id", "name"]
values = [101, "John"]

d = dict(zip(keys, values))

print(d)

Output:

{'id': 101, 'name': 'John'}
3. Unzipping
pairs = [(1, 'a'), (2, 'b')]

a, b = zip(*pairs)

print(a)
print(b)

Output:

(1, 2)
('a', 'b')
Internal Concept

zip() works like parallel iteration.

Pseudo logic:

i = 0

while items exist:
    take list1[i]
    take list2[i]
    make tuple
    i += 1
Data Types

Input:

list + list
tuple + list
set + tuple
string + list

All iterable combinations work.

Example:

list(zip("abc", [1,2,3]))

Output:

[('a', 1), ('b', 2), ('c', 3)]



"""

print(__doc__)

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

from pyspark import SparkContext,SparkConf
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

# create RDD

rdd=sc.parallelize( list(zip(list1,list2)) )
df=rdd.toDF(["column1","column2"])
df.show()