"""



imput:


output:

notes:

https://chatgpt.com/c/6a03ee1d-5298-8324-8cde-9eb9ffe29d36


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

from pyspark import SparkContext,SparkConf
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *

from pyspark.sql import Window

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

data = [
    (1, "John", 5000),
    (2, "Sam", 6000)
]

df = spark.createDataFrame(data, ["Emp ID", "Emp Name", "Salary"])

df.show()

from pyspark.sql.functions import *

# Renaming

# Emp ID   → emp_id
# Emp Name → emp_name
# Salary   → salary


# columns= [ col(column).alias('__'+column)   for column in df.columns ]


columns= [ col(column).alias(column.replace(" ","ZZZ"))  for column in df.columns ]
df.select(*columns).show()



