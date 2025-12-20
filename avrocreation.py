# Install fastavro if not already installed:
# pip install fastavro

import fastavro

# Define schema
schema = {
    "type": "record",
    "name": "MonthlySales",
    "fields": [
        {"name": "month", "type": "string"},
        {"name": "sales", "type": "int"},
        {"name": "profit", "type": "int"},
        {"name": "location", "type": "string"}
    ]
}

# Define data
records = [
    {"month": "january", "sales": 100000, "profit": 5000, "location": "delhi"},
    {"month": "february", "sales": 200000, "profit": 15000, "location": "delhi"},
    {"month": "march", "sales": 100000, "profit": 5000, "location": "delhi"},
    {"month": "april", "sales": 300000, "profit": 15000, "location": "delhi"},
    {"month": "may", "sales": 100000, "profit": 5000, "location": "delhi"},
    {"month": "june", "sales": 500000, "profit": 56000, "location": "delhi"},
    {"month": "july", "sales": 100000, "profit": 5000, "location": "delhi"},
    {"month": "august", "sales": 100000, "profit": 5000, "location": "delhi"},
    {"month": "september", "sales": 600000, "profit": 25000, "location": "delhi"},
    {"month": "october", "sales": 100000, "profit": 5000, "location": "delhi"},
    {"month": "november", "sales": 100000, "profit": 5000, "location": "delhi"},
    {"month": "december", "sales": 700000, "profit": 35000, "location": "delhi"}
]

# Save Avro file
with open("monthly_sales.avro", "wb") as out_file:
    fastavro.writer(out_file, schema, records)

print("Avro file 'monthly_sales.avro' has been created successfully!")
