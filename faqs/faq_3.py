"""
RevenueData
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



from pyspark.sql import SparkSession

from pyspark.sql.functions import  *

spark = SparkSession.builder.appName("JoinTest").getOrCreate()

product_data = [
    (1, "Laptop", "Electronics", 1000),
    (2, "Smartphone", "Electronics", 700),
    (3, "Office Chair", "Furniture", 150),
    (4, "Desk Lamp", "Furniture", 40),
    (5, "Headphones", "Accessories", 100)
]

product_columns = ["ProductID", "ProductName", "Category", "Price"]

product_df = spark.createDataFrame(product_data, product_columns)

product_df.show()

sales_data = [
    (101, 1, 2, "2025-08-01", "North"),
    (102, 2, 5, "2025-08-01", "South"),
    (103, 3, 1, "2025-08-02", "North"),
    (104, 2, 3, "2025-08-03", "East"),
    (105, 5, 4, "2025-08-04", "West"),
    (106, 1, 1, "2025-08-04", "East"),
    (107, 4, 2, "2025-08-04", "South")
]

sales_columns = ["SaleID", "ProductID", "Quantity", "SaleDate", "Region"]

sales_df = spark.createDataFrame(sales_data, sales_columns)

sales_df.show()

print("joined df")
joindf=sales_df.alias("s").join(product_df.alias("p"),["ProductID"],"inner")
joindf.show()


joindf.createOrReplaceTempView("sqld")
print("SPARK SQL")
spark.sql("""
select sum(quantity*Price) as sum from sqld
""").show()


spark.sql("""
select ProductID,sum(quantity*Price) as sum from sqld
group by ProductID
""").show()

spark.sql("""
select Region,sum(quantity*Price) as sum from sqld
group by Region
""").show()



# by DSL



joindf.groupby(col("Region")).agg(sum(col("quantity")*col("Price"))).show()