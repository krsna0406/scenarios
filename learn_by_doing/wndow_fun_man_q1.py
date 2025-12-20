"""
This module demonstrates how to calculate the minimum and maximum `sales`
value per product using PySpark window functions.

Key Concepts:
-------------
1. Window Specification:
- We partition the data by `product_id` so each product is processed separately.
- We order by `sales` (required for window functions).

2. Window Frame:
- rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
- This tells Spark to scan the *entire* partition (all rows for a product),
not just up to the current row.
- Without this, Spark defaults to a cumulative frame:
UNBOUNDED PRECEDING → CURRENT ROW
which gives incorrect max/min.

3. Result:
- `min_sales`: lowest sales value per product.
- `max_sales`: highest sales value per product.

scenario : FInd out the difference in sales of each product from their first month sales to latest sales

"""

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


print(__doc__)
from pyspark import SparkContext,SparkConf
from pyspark.sql import  SparkSession

from pyspark.sql.functions import *

conf=SparkConf().setAppName("s1").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()



from pyspark.sql import  Window

product_data = [
    (2,"samsung","01-01-1995",11000),
    (1,"iphone","01-02-2023",1300000),
    (2,"samsung","01-02-2023",1120000),
    (3,"oneplus","01-02-2023",1120000),
    (1,"iphone","01-03-2023",1600000),
    (2,"samsung","01-03-2023",1080000),
    (3,"oneplus","01-03-2023",1160000),
    (1,"iphone","01-01-2006",15000),
    (1,"iphone","01-04-2023",1700000),
    (2,"samsung","01-04-2023",8000),
    (3,"oneplus","01-04-2023",1170000),
    (1,"iphone","01-05-2023",1200000),
    (2,"samsung","01-05-2023",980000),
    (3,"oneplus","01-05-2023",1175000),
    (1,"iphone","01-06-2023",1100000),
    (3,"oneplus","01-01-2010",23000),
    (2,"samsung","01-06-2023",1100000),
    (3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

product_df.show()

# in traditional way to get the min and max of sales
#
# product_df2=product_df.groupby("product_name").agg(min ("sales_date").alias("min_date"),\
#                                                    max("sales_date").alias("max_date"))
#
# product_df2.show()

# window=Window.partitionBy("product_id").orderBy("sales_date")
#
# window = (
#     Window.partitionBy("product_id")
#     .orderBy("sales_date")\
#         # .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
# )
# # product_df.withColumn("sales_date", to_date("sales_date"))\
# #     .withColumn("min_date",min( "sales_date").over(window))\
# #     .withColumn("max_date",max("sales_date").over(window)).show(truncate=False)
# #
#
#
# #min_date  |max_date  |
#
# # result = (
# #     product_df.withColumn("sales_date", to_date("sales_date", "dd-MM-yyyy"))
# #     .withColumn("min_date", min("sales_date").over(window))
# #     .withColumn("max_date", max("sales_date").over(window))
# # )
#
# #min_sales |max_sales  |
#
# result = (
#     product_df.withColumn("min_sales", min("sales").over(window))
#     .withColumn("max_sales", max("sales").over(window))
# )
#
#
# result.show(truncate=False)


# scenario code

window = (
    Window.partitionBy("product_id")
    .orderBy("sales_date")\
        .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)
)

resultdf=(
    product_df.withColumn("first_sale", first("sales").over(window))
    .withColumn("last_sale",last("sales").over(window))
)
resultdf.show()


final_df=resultdf.withColumn("sale_diff",expr(" last_sale-first_sale "))
final_df.show()