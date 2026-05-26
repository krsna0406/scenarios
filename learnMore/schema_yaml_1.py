"""
YAML:


notes:  https://chatgpt.com/c/6a1427b7-f110-8321-a6a1-b6fb450979e5



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
#
# spark=SparkSession.builder\
#     .config("spark.sql.shuffle.partitions",3)\
#     .config("spark.sql.adaptive.enabled","false")\
#     .getOrCreate()
#
# import yaml
#
# with open("C:\\Users\\Krishna\\IdeaProjects\\kanna\\configs\\customer_schema.yaml") as f:
#     config = yaml.safe_load(f)
#
# expected_schema = config["schema"]
#
#
# print("expected_schema",expected_schema)


import yaml

from pyspark.sql import SparkSession


# -----------------------------------------
# Read YAML Configuration
# -----------------------------------------

def read_yaml(file_path):

    with open(file_path, "r") as file:
        return yaml.safe_load(file)


# -----------------------------------------
# Schema Validation
# -----------------------------------------

def validate_schema(df, expected_schema_dict):

    validation_errors = []

    # Build actual schema dictionarylik
    # using df.schema.fields

    actual_schema_dict = {
        field.name: type(field.dataType).__name__
        for field in df.schema.fields
    }

    actual_columns = set(actual_schema_dict.keys())
    expected_columns = set(expected_schema_dict.keys())

    # ---------------------------------
    # Missing Columns
    # ---------------------------------

    missing_columns = expected_columns - actual_columns

    if missing_columns:

        validation_errors.append(
            f"Missing Columns : {sorted(list(missing_columns))}"
        )

    # ---------------------------------
    # Extra Columns
    # ---------------------------------

    extra_columns = actual_columns - expected_columns

    if extra_columns:

        validation_errors.append(
            f"Extra Columns : {sorted(list(extra_columns))}"
        )

    # ---------------------------------
    # Datatype Validation
    # ---------------------------------

    common_columns = actual_columns.intersection(
        expected_columns
    )

    for col in common_columns:

        expected_type = expected_schema_dict[col]
        actual_type = actual_schema_dict[col]

        if expected_type != actual_type:

            validation_errors.append(
                f"Datatype Mismatch : "
                f"{col} "
                f"(Expected={expected_type}, "
                f"Actual={actual_type})"
            )

    return validation_errors


# -----------------------------------------
# Main
# -----------------------------------------

if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("SchemaValidation")
        .getOrCreate()
    )

    # Read YAML

    config = read_yaml("C:\\Users\\Krishna\\IdeaProjects\\kanna\\configs\\customer_schema.yaml")

    print("config >>>>>>>>>>>>>>>>>",config)
    source_path = config["source"]["path"]
    source_format = config["source"]["format"]
    header = config["source"]["header"]

    expected_schema = config["schema"]

    print("\nExpected Schema From YAML")
    print(expected_schema)

    # Read source file

    df = (
        spark.read
        .format(source_format)
        .option("header", header)
        .option("inferSchema", True)
        .load(source_path)
    )

    print("\nActual Schema")

    df.printSchema()

    # Validate Schema

    validation_errors = validate_schema(
        df,
        expected_schema
    )

    # Validation Result

    if validation_errors:

        print("\nSCHEMA VALIDATION FAILED\n")

        for error in validation_errors:
            print(error)

        raise Exception(
            "Schema Validation Failed"
        )

    else:

        print("\nSCHEMA VALIDATION SUCCESSFUL")

        df.show(truncate=False)

        # Continue ETL processing here

    spark.stop()