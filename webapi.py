from urllib.request import urlopen, Request
import json
from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder.appName("APIReadUsingUrllib").getOrCreate()

# API URL
api_url = "https://jsonplaceholder.typicode.com/posts"

# Create request
req = Request(api_url)

# Call API
with urlopen(req) as response:
    data = response.read().decode("utf-8")

# Parse JSON
data_json = json.loads(data)

# Convert to DataFrame
df = spark.createDataFrame(data_json)

df.show(truncate=False)
df.printSchema()
