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
from datetime import datetime

def create_audit_records(file_name,
                         missing_cols,
                         extra_cols,
                         datatype_mismatches):

    audit_rows = []

    current_time = datetime.now()

    for col_name in missing_cols:

        audit_rows.append(
            (
                file_name,
                "MISSING_COLUMN",
                col_name,
                None,
                None,
                "FAILED",
                current_time
            )
        )

    for col_name in extra_cols:

        audit_rows.append(
            (
                file_name,
                "EXTRA_COLUMN",
                col_name,
                None,
                None,
                "FAILED",
                current_time
            )
        )

    for col_name, src_type, ref_type in datatype_mismatches:

        audit_rows.append(
            (
                file_name,
                "DATATYPE_MISMATCH",
                col_name,
                src_type,
                ref_type,
                "FAILED",
                current_time
            )
        )

    if len(audit_rows) == 0:

        audit_rows.append(
            (
                file_name,
                "SCHEMA_VALIDATION",
                None,
                None,
                None,
                "PASSED",
                current_time
            )
        )

    return audit_rows