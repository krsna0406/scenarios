"""
word count

input:


output:

imp note:

SQL:

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
# Initialize Spark
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read text file
rdd = spark.sparkContext.textFile("input.txt")

# Split words, normalize to lowercase
words_rdd = rdd.flatMap(lambda line: line.lower().split())

# Map each word to (word, 1) and reduce by key
word_count_rdd = words_rdd.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)

# Collect and print
for word, count in word_count_rdd.collect():
    print(f"{word}: {count}")

