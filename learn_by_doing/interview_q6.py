"""

INTERVIEW QUESTION
2 question
1. WE HAVE TO flatten data from 1 row to multiple row eg . [1,2,3] we need to convert into row wise flattening
2. WE HAVE TO FIND OUT device pinged or not
SOLVE USING PYSPARK AND SPARK SQL

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
from pyspark.sql.functions import *
data = [(1,['mobile','PC','Tab']),(2,['mobile','PC']),(3,['Tab','Pen'])]
schema=['customer_id','product_purchase']
#
# df= spark.createDataFrame(data,schema)
# df.withColumn('product',explode('product_purchase')).select('customer_id','product')
#
# df.show()
#
# df.withColumn("product_purchase", explode(col("product_purchase"))).show()
#

# 4:
# find out not null value in the column


data=[(1, 'yes',None,None),(2, None,'yes',None),(3, 'No',None,'yes')]
schema=['customer_id','device_using1','device_using2','device_using3']

df1=spark.createDataFrame(data,schema)
df1.withColumn('new',coalesce(col('device_using1'),col('device_using2'),col('device_using3'))).show()