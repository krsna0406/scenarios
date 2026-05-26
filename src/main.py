"""

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

spark = SparkSession.builder.appName("SchemaValidation").getOrCreate()
reference_df = (
    spark.read
    .option("header", True)
    .csv("C:\\Users\\Krishna\\IdeaProjects\\kanna\\configs\\reference_data.csv")
)
reference_df.show(truncate=False)
control_df = (
    spark.read
    .option("header", True)
    .csv("C:\\Users\\Krishna\\IdeaProjects\\kanna\\configs\\control_file.csv")
)

control_df.show(truncate=False)



from pyspark.sql import SparkSession
from pyspark.sql.types import *

from file_reader import read_file
from schema_validator import compare_schema
from audit import create_audit_records

spark = (
    SparkSession.builder
    .appName("SchemaValidationFramework")
    .getOrCreate()
)

reference_df = (
    spark.read
    .option("header", True)
    .csv("/configs/reference_schema.csv")
)

control_df = (
    spark.read
    .option("header", True)
    .csv("/configs/control_file.csv")
)

audit_schema = StructType([
    StructField("file_name", StringType()),
    StructField("validation_type", StringType()),
    StructField("column_name", StringType()),
    StructField("source_type", StringType()),
    StructField("reference_type", StringType()),
    StructField("status", StringType()),
    StructField("validation_timestamp", TimestampType())
])

all_audit_records = []

for row in control_df.collect():

    file_name = row["filename"]
    file_path = row["filepath"]

    print(f"Processing {file_name}")

    source_df = read_file(
        spark,
        file_path
    )

    file_reference_df = (
        reference_df
        .filter(reference_df.SrcFileName == file_name)
    )

    (
        missing_cols,
        extra_cols,
        datatype_mismatches
    ) = compare_schema(
        source_df,
        file_reference_df,
        file_name
    )

    audit_records = create_audit_records(
        file_name,
        missing_cols,
        extra_cols,
        datatype_mismatches
    )

    all_audit_records.extend(audit_records)

audit_df = spark.createDataFrame(
    all_audit_records,
    audit_schema
)

audit_df.show(truncate=False)

# audit_df.write.mode("overwrite").parquet(
#     "s3://audit-bucket/schema_validation_report/"
# )