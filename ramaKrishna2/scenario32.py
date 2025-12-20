"""

scenario 32:
input:

+-------+-------------------+
|food_id|          food_item|
+-------+-------------------+
|      1|        Veg Biryani|
|      2|     Veg Fried Rice|
|      3|    Kaju Fried Rice|
|      4|    Chicken Biryani|
|      5|Chicken Dum Biryani|
|      6|     Prawns Biryani|
|      7|      Fish Birayani|
+-------+-------------------+

+-------+------+
|food_id|rating|
+-------+------+
|      1|     5|
|      2|     3|
|      3|     4|
|      4|     4|
|      5|     5|
|      6|     4|
|      7|     4|
+-------+------+


expected :

+-------+-------------------+------+---------------+
|food_id|          food_item|rating|stats(out of 5)|
+-------+-------------------+------+---------------+
|      1|        Veg Biryani|     5|          *****|
|      2|     Veg Fried Rice|     3|            ***|
|      3|    Kaju Fried Rice|     4|           ****|
|      4|    Chicken Biryani|     4|           ****|
|      5|Chicken Dum Biryani|     5|          *****|
|      6|     Prawns Biryani|     4|           ****|
|      7|      Fish Birayani|     4|           ****|
+-------+-------------------+------+---------------+



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
from pyspark.sql.functions import repeat, col

conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
sc=SparkContext(conf=conf)

spark=SparkSession.builder.getOrCreate()

data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]

df1 = spark.createDataFrame(data,["food_id","food_item"])
df1.show()

ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]

df2 = spark.createDataFrame(ratings,["food_id","rating"])
df2.show()

joindf=df1.join(df2,["food_id"],"inner")

# joindf.withColumn("stats(out of 5)", repeat("*",col("rating").cast(IntegerType()))).show()
#
print(repeat)
joindf.printSchema()

finaldf = joindf.withColumn("stats(out of 5)",expr("repeat('*',rating)"))
finaldf.show()
#
# joindf.withColumn("stars",repeat(lit("*"), col("rating").cast(IntegerType()))).show()



