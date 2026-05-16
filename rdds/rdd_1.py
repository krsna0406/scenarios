"""
RDDs faqs
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

# RDD operations
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

# 1. Create RDD (Basics)
config=SparkConf().setAppName("rdd1").setMaster("local[*]")
sc=SparkContext(conf=config)
rdd=sc.parallelize([1,2,3,4,5])
print(rdd)

print(rdd.collect())
# 2. Map Transformation
print(rdd.map(lambda x: x+10).collect())
# 3. Filter Transformation

fltrrdd=rdd.filter(lambda x: x>4)
print(fltrrdd.collect())


# 4. FlatMap

rdd2=sc.parallelize(['abc xyz','stu zxs'])

flatrdd=rdd2.flatMap(lambda x: x.split(" "))
print(flatrdd.collect())

# 5. Reduce Action


rdd3=sc.parallelize([1,2,3,4,5,6,7])

redVal=rdd3.reduce(lambda x,y: x+y )

print(redVal)

# 6. Key-Value RDD


pairRdd= sc.parallelize([(1,2),(3,4),(5,6)])

print(pairRdd.collect())

redv1= pairRdd.reduce(lambda x,y: x+y)
print(redv1)
# What does x + y mean here?
#
# Since x and y are tuples, Python applies tuple concatenation, NOT arithmetic addition.

print("paired RDD reduceBYKey")

kvrdd=sc.parallelize([('a',1),('b',1),('a',2)])

redBYK=kvrdd.reduceByKey(lambda x,y :x+y)

print(redBYK.collect())

# 7. Word Count

print("word count")
rdd = sc.parallelize(["hello world", "hello spark"])

resrdd=rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y).collect()

print(resrdd)

for i in resrdd:
    print("{} {}".format(i[0],i[1]))

# 8. Distinct & Count

print("distinct and count")
rdd = sc.parallelize([1, 2, 2, 3, 3, 3])
print(rdd.distinct().collect())
print(rdd.count())


# 9. GroupByKey vs ReduceByKey


rdd = sc.parallelize([("a", 1), ("a", 2), ("b", 3)])

gbyrdd=rdd.groupByKey()

print(gbyrdd.collect())

#
# grouped = rdd.groupByKey().mapValues(list)
# print(grouped.collect())

print(rdd.groupByKey().mapValues(list).collect())


# 10. Sort By Key
print("Sort By Key")
rdd = sc.parallelize([("a", 3), ("c", 1), ("b", 2)])
sorted_rdd = rdd.sortByKey()
print(sorted_rdd.collect())

# 11. Join Operation
print("11. Join Operation")
rdd1 = sc.parallelize([("a", 1), ("b", 2)])
rdd2 = sc.parallelize([("a", 3), ("b", 4)])

joined = rdd1.join(rdd2)
print(joined.collect())


# 12. Cache / Persist (Performance)


print("12. Cache / Persist (Performance)")

rdd = sc.parallelize(range(1000000))
rdd.cache()

print(rdd.count())
print(rdd.count())


# 13. Partition Understanding

print("13. Partition Understanding")
rdd = sc.parallelize(range(10), 3)
print(rdd.getNumPartitions())


# 14. Take vs Collect

print("14. Take vs Collect")

rdd = sc.parallelize(range(100))
print(rdd.take(5))

# 15. Real-World Example (Log Analysis)
print("15. Real-World Example (Log Analysis)")

logs = sc.parallelize([
    "ERROR disk failure",
    "INFO job started",
    "ERROR timeout",
    "INFO job completed"
])

error_count = (logs
               .filter(lambda x: "ERROR" in x)
               .count())

print(error_count)



