import os
import sys
import subprocess
# Match PySpark to the running Python interpreter
#os.environ["PYSPARK_PYTHON"] = sys.executable

print(sys.executable)
#os.environ["JAVA_HOME"] = r'C:\Users\Krishna\.jdks\ms-17.0.16'
#os.environ["HADOOP_HOME"] = r'C:\app\zeyoplus\soft\sw\hadoop'

# Update PATH
#os.environ["PATH"] = java_home + "/bin:" + os.environ["PATH"]
from pyspark.sql import SparkSession
print("welcome krishna")
# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()
# Step 2: Define data
data = [
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 35)
]
# Step 3: Define schema (column names)
columns = ["Name", "Age"]

# Step 4: Create DataFrame
df = spark.createDataFrame(data, columns)

# Step 5: Show DataFrame
df.show()
df.printSchema()
