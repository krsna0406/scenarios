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
from pyspark.sql.functions import current_timestamp


def compare_schema(source_df,
                   reference_df,
                   file_name):

    source_schema = {
        field.name.lower(): field.dataType.simpleString()
        for field in source_df.schema.fields
    }

    reference_schema = {
        row["SrcColumns"].lower():
            row["SrcColumnType"].lower()

        for row in reference_df.collect()
    }

    source_cols = set(source_schema.keys())
    reference_cols = set(reference_schema.keys())

    missing_columns = reference_cols - source_cols

    extra_columns = source_cols - reference_cols

    common_columns = source_cols.intersection(reference_cols)

    datatype_mismatches = []

    for col_name in common_columns:

        src_type = source_schema[col_name]
        ref_type = reference_schema[col_name]

        if src_type != ref_type:

            datatype_mismatches.append(
                (
                    col_name,
                    src_type,
                    ref_type
                )
            )

    return (
        missing_columns,
        extra_columns,
        datatype_mismatches
    )