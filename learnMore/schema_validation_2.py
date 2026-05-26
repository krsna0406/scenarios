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

# conf=SparkConf().setAppName("scenario1").setMaster("local[*]")
# sc=SparkContext(conf=conf)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType, LongType
)
from pyspark.sql.functions import col, lit

# ─────────────────────────────────────────────
# 1. Spark Session
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SchemaComparison") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ─────────────────────────────────────────────
# 2. Define Reference Schema (Source / Expected)
# ─────────────────────────────────────────────
reference_schema = StructType([
    StructField("customer_id",    IntegerType(),  nullable=False),
    StructField("customer_name",  StringType(),   nullable=False),
    StructField("email",          StringType(),   nullable=True),
    StructField("phone",          StringType(),   nullable=True),
    StructField("age",            IntegerType(),  nullable=True),
    StructField("salary",         DoubleType(),   nullable=True),
    StructField("join_date",      DateType(),     nullable=True),
    StructField("is_active",      BooleanType(),  nullable=True),
    StructField("account_balance",DoubleType(),   nullable=True),
    StructField("region",         StringType(),   nullable=True),
])

# ─────────────────────────────────────────────
# 3. Define Control Schema (Target / Actual CSV)
#    Intentionally has differences for demo
# ─────────────────────────────────────────────
control_schema = StructType([
    StructField("customer_id",    LongType(),     nullable=False),   # type mismatch
    StructField("customer_name",  StringType(),   nullable=False),
    StructField("email",          StringType(),   nullable=False),   # nullability mismatch
    StructField("phone",          StringType(),   nullable=True),
    StructField("age",            StringType(),   nullable=True),    # type mismatch
    StructField("salary",         DoubleType(),   nullable=True),
    # join_date MISSING
    StructField("is_active",      BooleanType(),  nullable=True),
    StructField("account_balance",DoubleType(),   nullable=True),
    StructField("region",         StringType(),   nullable=True),
    StructField("country",        StringType(),   nullable=True),    # extra column
])


# ─────────────────────────────────────────────
# 4. Sample Reference Dataset
# ─────────────────────────────────────────────
ref_data = [
    (1,  "Ravi Kumar",    "ravi@mail.com",   "9876543210", 32, 55000.0, "2021-01-15", True,  12000.50, "South"),
    (2,  "Priya Sharma",  "priya@mail.com",  "9123456789", 28, 72000.0, "2020-06-01", True,  45000.00, "North"),
    (3,  "Arun Reddy",    None,              "9988776655", 45, 90000.0, "2019-03-20", False,  8000.75, "East"),
    (4,  "Sunita Rao",    "sunita@mail.com", None,         35, 61000.0, "2022-11-10", True,  23000.00, "West"),
    (5,  "Vikram Singh",  "vikram@mail.com", "9001234567", 50, 110000.0,"2018-07-05", True, 100000.00, "North"),
    (6,  "Meena Patel",   "meena@mail.com",  "9876501234", 29, 48000.0, "2023-02-28", True,   5500.00, "South"),
    (7,  "Deepak Joshi",  None,              "9765432100", 38, 83000.0, "2020-09-14", False, 17000.25, "East"),
    (8,  "Anjali Nair",   "anjali@mail.com", "9654321098", 26, 41000.0, "2023-07-01", True,   2300.00, "South"),
    (9,  "Suresh Babu",   "suresh@mail.com", None,         55, 125000.0,"2017-04-18", True, 200000.00, "West"),
    (10, "Kavitha Menon", "kavi@mail.com",   "9543210987", 33, 67000.0, "2021-08-22", True,  34000.00, "North"),
]

ref_columns = ["customer_id","customer_name","email","phone","age",
               "salary","join_date","is_active","account_balance","region"]

ref_df = spark.createDataFrame(ref_data, schema=ref_columns)
ref_df = ref_df.withColumn("join_date", col("join_date").cast(DateType()))
ref_df = ref_df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
ref_df = ref_df.withColumn("age", col("age").cast(IntegerType()))
print("✅ Reference Dataset:")
ref_df.printSchema()
ref_df.show(5, truncate=False)


# ─────────────────────────────────────────────
# 5. Sample Control Dataset (CSV-like)
# ─────────────────────────────────────────────
ctrl_data = [
    (1,  "Ravi Kumar",    "ravi@mail.com",   "9876543210", "32",  55000.0, True,  12000.50, "South", "India"),
    (2,  "Priya Sharma",  "priya@mail.com",  "9123456789", "28",  72000.0, True,  45000.00, "North", "India"),
    (3,  "Arun Reddy",    "arun@mail.com",   "9988776655", "45",  90000.0, False,  8000.75, "East",  "India"),
    (4,  "Sunita Rao",    "sunita@mail.com", "9999999999", "35",  61000.0, True,  23000.00, "West",  "India"),
    (5,  "Vikram Singh",  "vikram@mail.com", "9001234567", "50", 110000.0, True, 100000.00, "North", "India"),
    (6,  "Meena Patel",   "meena@mail.com",  "9876501234", "29",  48000.0, True,   5500.00, "South", "India"),
    (7,  "Deepak Joshi",  "deepak@mail.com", "9765432100", "38",  83000.0, False, 17000.25, "East",  "India"),
    (8,  "Anjali Nair",   "anjali@mail.com", "9654321098", "26",  41000.0, True,   2300.00, "South", "India"),
    (9,  "Suresh Babu",   "suresh@mail.com", "9543210000", "55", 125000.0, True, 200000.00, "West",  "India"),
    (10, "Kavitha Menon", "kavi@mail.com",   "9543210987", "33",  67000.0, True,  34000.00, "North", "India"),
]

ctrl_columns = ["customer_id","customer_name","email","phone","age",
                "salary","is_active","account_balance","region","country"]

ctrl_df = spark.createDataFrame(ctrl_data, schema=ctrl_columns)
ctrl_df = ctrl_df.withColumn("customer_id", col("customer_id").cast(LongType()))
print("\n✅ Control Dataset:")
ctrl_df.printSchema()
ctrl_df.show(5, truncate=False)


# ─────────────────────────────────────────────
# 6. Schema Comparison Function
# ─────────────────────────────────────────────
def compare_schemas(ref_df, ctrl_df, ref_name="Reference", ctrl_name="Control"):
    """
    Compares two DataFrame schemas and returns:
      - matched columns
      - type mismatches
      - nullability mismatches
      - columns only in reference
      - columns only in control
    """
    ref_fields  = {f.name: f for f in ref_df.schema.fields}
    ctrl_fields = {f.name: f for f in ctrl_df.schema.fields}

    ref_cols  = set(ref_fields.keys())
    ctrl_cols = set(ctrl_fields.keys())

    only_in_ref   = sorted(ref_cols - ctrl_cols)
    only_in_ctrl  = sorted(ctrl_cols - ref_cols)
    common_cols   = sorted(ref_cols & ctrl_cols)

    matched        = []
    type_mismatch  = []
    null_mismatch  = []

    for col_name in common_cols:
        rf = ref_fields[col_name]
        cf = ctrl_fields[col_name]
        type_ok = (str(rf.dataType) == str(cf.dataType))
        null_ok = (rf.nullable == cf.nullable)

        if type_ok and null_ok:
            matched.append(col_name)
        else:
            if not type_ok:
                type_mismatch.append({
                    "column":     col_name,
                    f"{ref_name}_type":  str(rf.dataType),
                    f"{ctrl_name}_type": str(cf.dataType),
                })
            if not null_ok:
                null_mismatch.append({
                    "column":       col_name,
                    f"{ref_name}_nullable":  rf.nullable,
                    f"{ctrl_name}_nullable": cf.nullable,
                })

    # ── Print Report ──────────────────────────
    sep = "=" * 60
    print(f"\n{sep}")
    print("          SCHEMA COMPARISON REPORT")
    print(sep)

    print(f"\n✅ Perfectly Matched Columns ({len(matched)}):")
    for c in matched:
        print(f"   • {c}")

    print(f"\n🔴 Type Mismatches ({len(type_mismatch)}):")
    if type_mismatch:
        for m in type_mismatch:
            print(f"   • {m['column']}")
            print(f"       {ref_name}  : {m[f'{ref_name}_type']}")
            print(f"       {ctrl_name} : {m[f'{ctrl_name}_type']}")
    else:
        print("   None")

    print(f"\n🟡 Nullability Mismatches ({len(null_mismatch)}):")
    if null_mismatch:
        for m in null_mismatch:
            print(f"   • {m['column']}")
            print(f"       {ref_name}  nullable: {m[f'{ref_name}_nullable']}")
            print(f"       {ctrl_name} nullable: {m[f'{ctrl_name}_nullable']}")
    else:
        print("   None")

    print(f"\n🟠 Columns ONLY in {ref_name} (Missing in {ctrl_name}) ({len(only_in_ref)}):")
    for c in only_in_ref:
        dtype = str(ref_fields[c].dataType)
        print(f"   • {c} ({dtype})")
    if not only_in_ref:
        print("   None")

    print(f"\n🔵 Columns ONLY in {ctrl_name} (Extra vs {ref_name}) ({len(only_in_ctrl)}):")
    for c in only_in_ctrl:
        dtype = str(ctrl_fields[c].dataType)
        print(f"   • {c} ({dtype})")
    if not only_in_ctrl:
        print("   None")

    total_issues = len(type_mismatch) + len(null_mismatch) + len(only_in_ref) + len(only_in_ctrl)
    print(f"\n{'─'*60}")
    print(f"  Total Issues Found : {total_issues}")
    print(f"  Schema Match Status: {'✅ PASS' if total_issues == 0 else '❌ FAIL'}")
    print(sep + "\n")

    return {
        "matched":       matched,
        "type_mismatch": type_mismatch,
        "null_mismatch": null_mismatch,
        "only_in_ref":   only_in_ref,
        "only_in_ctrl":  only_in_ctrl,
        "status":        "PASS" if total_issues == 0 else "FAIL",
    }


# ─────────────────────────────────────────────
# 7. Run Comparison
# ─────────────────────────────────────────────
result = compare_schemas(ref_df, ctrl_df, ref_name="Reference", ctrl_name="Control")


# ─────────────────────────────────────────────
# 8. Export Comparison Report as DataFrame
# ─────────────────────────────────────────────
def build_report_df(spark, result):
    rows = []
    for c in result["matched"]:
        rows.append((c, "MATCH", "–", "–", "–", "–"))
    for m in result["type_mismatch"]:
        rows.append((m["column"], "TYPE_MISMATCH",
                     m.get("Reference_type",""), m.get("Control_type",""), "–", "–"))
    for m in result["null_mismatch"]:
        rows.append((m["column"], "NULLABILITY_MISMATCH", "–", "–",
                     str(m.get("Reference_nullable","")),
                     str(m.get("Control_nullable",""))))
    for c in result["only_in_ref"]:
        rows.append((c, "MISSING_IN_CONTROL", "–", "–", "–", "–"))
    for c in result["only_in_ctrl"]:
        rows.append((c, "EXTRA_IN_CONTROL", "–", "–", "–", "–"))

    report_schema = StructType([
        StructField("column_name",       StringType(), True),
        StructField("status",            StringType(), True),
        StructField("ref_type",          StringType(), True),
        StructField("ctrl_type",         StringType(), True),
        StructField("ref_nullable",      StringType(), True),
        StructField("ctrl_nullable",     StringType(), True),
    ])
    return spark.createDataFrame(rows, schema=report_schema)

report_df = build_report_df(spark, result)
print("📋 Schema Comparison Report DataFrame:")
report_df.show(truncate=False)

# Save report as CSV
report_df.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("schema_comparison_report")
print("💾 Report saved to: schema_comparison_report/")


# ─────────────────────────────────────────────
# 9. Data-Level Comparison (common columns only)
# ─────────────────────────────────────────────
common_cols = [c for c in result["matched"]
               if c not in [m["column"] for m in result["type_mismatch"]]]

# Cast control to match ref types for common cols
ctrl_aligned = ctrl_df.select([col(c) for c in common_cols])
ref_aligned  = ref_df.select([col(c) for c in common_cols])

# Rows in ref but not in control
in_ref_not_ctrl = ref_aligned.subtract(ctrl_aligned)
# Rows in control but not in ref
in_ctrl_not_ref = ctrl_aligned.subtract(ref_aligned)

print(f"\n🔍 Data-Level Comparison (on {len(common_cols)} matched columns):")
print(f"   Rows in Reference NOT in Control : {in_ref_not_ctrl.count()}")
print(f"   Rows in Control NOT in Reference : {in_ctrl_not_ref.count()}")

if in_ref_not_ctrl.count() > 0:
    print("\n   Sample rows in Reference but not Control:")
    in_ref_not_ctrl.show(5, truncate=False)

if in_ctrl_not_ref.count() > 0:
    print("\n   Sample rows in Control but not Reference:")
    in_ctrl_not_ref.show(5, truncate=False)

spark.stop()
print("\n🏁 Schema Comparison Complete.")