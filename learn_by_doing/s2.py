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

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()

# FILL AND FILL NA

data = [
    (1, 704,     "STANDARD", None,                    "PR", 30100),
    (2, 704,     None,       "PASEO COSTA DEL SUR",   "PR", None),
    (3, 709,     None,       "BDA SAN LUIS",          "PR", 3700),
    (4, 76166,   "UNIQUE",   "CINGULAR WIRELESS",     "TX", 84000),
    (5, 76177,   "STANDARD", None,                    "TX", None)
]

columns = ["id", "zipcode", "type", "city", "state", "population"]

df = spark.createDataFrame(data, columns)
df.printSchema()
df.show(truncate=False)
df.fillna(0).show()

df.na.fill(1111,["population"]).show()

print("population [string fill in integer column]")

df.na.fill("other",['population']).show()

print("population [string fill in integer column after casting to string type]")

df1=df.withColumn("population",col("population").cast(StringType())).\
    na.fill("other",['population'])
df1.show()
print("""   "integer"  in cast""")


df1.withColumn("population",col("population").cast("integer")). \
    na.fill("other",['population']).show()


print(df.columns)



columns=df.columns

# in reverse order
df.select(columns[::-1]).show()

# renaming the columns dynamically by using the reduce:
# reduce(function, iterable, initial_value)
from functools import reduce

renamed_df=reduce(
    lambda df,i: df.withColumnRenamed(i,i+ "one")
    ,columns,
    df
)
renamed_df.show()


## SORT AND ORDER BY

# print("df sort by ")
renamed_df.sort("idone")
# print("df orderby ")

renamed_df.printSchema()
renamed_df.orderBy(col("idone").desc()).show()



# print("columns renaming dynamically")
# renamed_df1 = reduce(
#     lambda df, i: df.withColumnRenamed(i, i + "one"),
#     columns,
#     df
# )


