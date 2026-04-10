"""
complete PySpark solution to drop columns with more than 80% nulls from a DataFrame,
along with explanation:

input:


output:

imp note:

Explanation
1.	count(when(col(c).isNull(), c)) counts nulls per column.
2.	Divide by total_rows → gives percentage of nulls.
3.	Filter columns where null percentage > 0.8 → cols_to_drop.
4.	Drop those columns with df.drop(*cols_to_drop).


and note on column having all the None values::
got th below error so used schema while creating the df

pyspark.errors.exceptions.base.PySparkValueError:
[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring
Common Cause

Usually occurs when a column contains only None / null values or mixed types.

Example that causes the error:

data = [
    (1, "A", None),
    (2, "B", None)
]

df = spark.createDataFrame(data, ["id","name","salary"])

Here salary column is all None, so Spark cannot infer the datatype.

Solution 1 (Best) — Define Schema Manually
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

df.show()



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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Initialize Spark
spark = SparkSession.builder.appName("DropColumnsWithNulls").getOrCreate()

# Sample data
data = [
    (1, None, 10, None),
    (2, None, None, None),
    (3, 5, None, None),
    (4, None, None, None)
]
# columns = ["col1", "col2", "col3", "col4"]

schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True),
    StructField("col3", StringType(), True),
    StructField("col4", StringType(), True)
])

df = spark.createDataFrame(data, schema)
print("Original DataFrame:")
df.show()

# Total number of rows
total_rows = df.count()
# Compute null percentage per column
null_percentage = df.select([
    (count(when(col(c).isNull(), c)) / total_rows).alias(c)
    for c in df.columns
])
# Collect null percentages to driver
null_pct_dict = null_percentage.collect()[0].asDict()
# Columns to drop (more than 80% nulls)
cols_to_drop = [col_name for col_name, pct in null_pct_dict.items() if pct > 0.8]
print(f"Columns to drop: {cols_to_drop}")
# Drop columns
df_clean = df.drop(*cols_to_drop)
print("DataFrame after dropping columns:")
df_clean.show()

