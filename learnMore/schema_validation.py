"""
# DDL schema string
schema_ddl = "name STRING, age INT, salary DOUBLE"

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

# conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
# sc=SparkContext(conf=conf)

spark=SparkSession.builder \
    .config("spark.sql.shuffle.partitions",3) \
    .config("spark.sql.adaptive.enabled","false") \
    .getOrCreate()

data = [
    ("Alice", 25, 50000.0),
    ("Bob", 30, 60000.0),
    ("Charlie", 35, 70000.0)
]

# DDL schema string
schema_ddl = "name STRING, age INT, salary DOUBLE"

df = spark.createDataFrame(data, schema=schema_ddl)

df.show()
df.printSchema()



# schema comparisions

# expected_schema = {
#     "id": "int",
#     "name": "string",
#     "salary": "string",
#     "city": "string"
# }
#
# # print(df.schema.fields)
# # print(df.schema.fieldNames())
#
#
# actual_schema = {
#     field.name: field.dataType.simpleString()
#     for field in df.schema.fields
# }
#
#
# print("actual_schema---",actual_schema)
# print("expected_schema---",expected_schema)
#
#
# missing_cols = []
# extra_cols = []
# datatype_mismatch = []


#
# # Missing + datatype validation
# for col, dtype in expected_schema.items():
#
#     if col not in actual_schema:
#         missing_cols.append(col)
#
#     elif actual_schema[col] != dtype:
#         datatype_mismatch.append(
#             (col, dtype, actual_schema[col])
#         )
#
# # Extra columns
# for col in actual_schema:
#     if col not in expected_schema:
#         extra_cols.append(col)
#
# print("Missing Columns:", missing_cols)
# print("Extra Columns:", extra_cols)
# print("Datatype Mismatch:", datatype_mismatch)

expected_schema = {
    "id": "int",
    "name": "string",
    "salary": "string",
    "city": "string"
}


def validate_schema(df, expected_schema):

    actual_schema = {
        field.name.lower(): field.dataType.simpleString()
        for field in df.schema.fields
    }

    expected_schema = {
        k.lower(): v.lower()
        for k, v in expected_schema.items()
    }

    missing_cols = []
    extra_cols = []
    datatype_mismatch = []

    for col, dtype in expected_schema.items():

        if col not in actual_schema:
            missing_cols.append(col)

        elif actual_schema[col] != dtype:
            datatype_mismatch.append({
                "column": col,
                "expected": dtype,
                "actual": actual_schema[col]
            })

    for col in actual_schema:
        if col not in expected_schema:
            extra_cols.append(col)

    validation_status = ( len(missing_cols) == 0 and  len(extra_cols) == 0 and  len(datatype_mismatch) == 0 )

    return {
        "status": validation_status,
        "missing_columns": missing_cols,
        "extra_columns": extra_cols,
        "datatype_mismatch": datatype_mismatch
    }


result = validate_schema(df, expected_schema)

print(result)