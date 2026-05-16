"""
without pivot ..conditional aggregations VVVIMP


input:

+---------+---------+-----+----+
|StudentId|  Subject|Marks|Year|
+---------+---------+-----+----+
|        1|  English|   90|2024|
|        1|    Maths|   91|2024|
|        1|  Physics|   92|2024|
|        1|Chemistry|   93|2024|
|        1|  Physics|   98|2025|
|        1|Chemistry|   87|2025|
|        1|  English|   92|2025|
|        1|    Maths|   95|2025|
|        1|  Biology|   70|2024|
+---------+---------+-----+----+

output:

| StudentId | English | Maths | Physics | Chemistry | Biology | Year |
| --------- | ------- | ----- | ------- | --------- | ------- | ---- |
| 1         | 90      | 91    | 92      | 93        | 70      | 2024 |
| 1         | 92      | 95    | 98      | 87        | 0       | 2025 |


imp note:

2️⃣ pivot() — ⚠ Important

pivot() expects a column name string, not a Column expression.

Correct usage per Spark API:

pivot("Subject")

When you write: IMPPPPPPPPPPPP  dont use col in pivot

pivot(col("Subject"))




explanation:

Why do we use MAX() (or MIN()) in conditional aggregation?

Short answer:
👉 Because after GROUP BY, every selected column must be aggregated, and MAX() is a convenient way to extract a single non-null value from a conditional expression.

🔹 The core problem

When you write something like:

CASE WHEN rank = 1 THEN name END

This produces:

value for matching rows
NULL for non-matching rows

Now if you GROUP BY something, you’ll have multiple rows per group, like:

dept	name	rank
IT	A	1
IT	B	2
IT	C	3

After applying:

CASE WHEN rank = 1 THEN name END

You get:

dept	value
IT	A
IT	NULL
IT	NULL
🔹 Why MAX() works

When you apply:

MAX(CASE WHEN rank = 1 THEN name END)

👉 MAX() ignores NULLs and picks the only non-null value → "A"

So result becomes:

IT → A
🔹 Conceptual understanding

Think of it like:

"From all rows in this group, give me the one value where condition is true."

Since only one row satisfies the condition:

MAX() = that value
MIN() = also that value
🔹 Why not just use CASE without MAX?

Because this is invalid:

SELECT dept,
       CASE WHEN rank = 1 THEN name END
FROM table
GROUP BY dept

❌ Error: column not in GROUP BY or aggregate

🔹 Typical use case (Pivot-like transformation)
SELECT dept,
       MAX(CASE WHEN rank = 1 THEN name END) AS first_emp,
       MAX(CASE WHEN rank = 2 THEN name END) AS second_emp
FROM table
GROUP BY dept

👉 This converts rows → columns (very common in ETL / reporting)

🔹 Why specifically MAX (not SUM, COUNT)?
Function	Why not used
SUM	Works only for numbers
COUNT	Gives count, not value
AVG	Not meaningful for strings
MAX / MIN	✅ Works for strings & numbers, ignores NULL


*******************************




vvimp explanation:

case applies first then group happens ...



Expression we are explaining
        CASE WHEN Subject = 'English' THEN Marks ELSE 0 END

👉 Meaning:

If the row is English, keep the marks
Otherwise, put 0
🔹 Step 1: Original Data (focus on 2024 only)

| StudentId | Subject   | Marks | Year |
| --------- | --------- | ----- | ---- |
| 1         | English   | 90    | 2024 |
| 1         | Maths     | 91    | 2024 |
| 1         | Physics   | 92    | 2024 |
| 1         | Chemistry | 93    | 2024 |
| 1         | Biology   | 70    | 2024 |



🔹 Step 2: Apply CASE WHEN (row by row)

We create a new column (English_col) using:

CASE WHEN Subject = 'English' THEN Marks ELSE 0 END

Intermediate Table:

| StudentId | Subject   | Marks | Year | English_col |
| --------- | --------- | ----- | ---- | ----------- |
| 1         | English   | 90    | 2024 | 90          |
| 1         | Maths     | 91    | 2024 | 0           |
| 1         | Physics   | 92    | 2024 | 0           |
| 1         | Chemistry | 93    | 2024 | 0           |
| 1         | Biology   | 70    | 2024 | 0           |


👉 Only the English row keeps value, rest become 0.

🔹 Step 3: Grouping + Aggregation

Now we do:

GROUP BY StudentId, Year

and apply:

MAX(English_col)
Why MAX works?
English_col values per group
90, 0, 0, 0, 0

👉 MAX = 90

🔹 Step 4: Result after aggregation

| StudentId | Year | English |
| --------- | ---- | ------- |
| 1         | 2024 | 90      |


🔹 Same process for 2025
Before aggregation:
| Subject   | Marks | English_col |
| --------- | ----- | ----------- |
| Physics   | 98    | 0           |
| Chemistry | 87    | 0           |
| English   | 92    | 92          |
| Maths     | 95    | 0           |

After MAX:
English = 92



?????????????????????????????????????????????????????????????????????????????

question :

how two case statements intermediate tables happen ?


Both CASE expressions are evaluated in the same row-level pass, not separately.
👉 Then GROUP BY aggregates both columns together.

Let’s walk it step by step with intermediate tables.

🔹 Step 1: Input (2024 only for clarity)

| StudentId | Subject   | Marks | Year |
| --------- | --------- | ----- | ---- |
| 1         | English   | 90    | 2024 |
| 1         | Maths     | 91    | 2024 |
| 1         | Physics   | 92    | 2024 |
| 1         | Chemistry | 93    | 2024 |
| 1         | Biology   | 70    | 2024 |

🔹 Step 2: Evaluate BOTH CASE expressions (same time)
MAX(CASE WHEN Subject = 'English' THEN Marks ELSE 0 END) AS English,
MAX(CASE WHEN Subject = 'Maths' THEN Marks ELSE 0 END) AS Maths

Engine evaluates both CASEs per row, producing two derived columns:
Intermediate Table (very important)

| StudentId | Subject   | Marks | Year | English_col | Maths_col |
| --------- | --------- | ----- | ---- | ----------- | --------- |
| 1         | English   | 90    | 2024 | 90          | 0         |
| 1         | Maths     | 91    | 2024 | 0           | 91        |
| 1         | Physics   | 92    | 2024 | 0           | 0         |
| 1         | Chemistry | 93    | 2024 | 0           | 0         |
| 1         | Biology   | 70    | 2024 | 0           | 0         |

👉 Key point:

English row → only English_col has value
Maths row → only Maths_col has value
🔹 Step 3: GROUP BY
GROUP BY StudentId, Year

Group formed:

(1, 2024)
🔹 Step 4: Aggregation happens column-wise

Now aggregation is applied independently per column:

For English:
[90, 0, 0, 0, 0] → MAX = 90
For Maths:
[0, 91, 0, 0, 0] → MAX = 91
🔹 Step 5: Final Output
StudentId	Year	English	Maths
1	2024	90	91
🔹 Very Important Insight

👉 There are NOT separate intermediate tables per CASE
👉 Instead, think:

One table → multiple derived columns → one GROUP BY → multiple aggregations
🔹 Mental Model (clean)
Row Processing:
   create English_col
   create Maths_col

Grouping:
   group rows

Aggregation:
   MAX(English_col)
   MAX(Maths_col)
🔹 Interview-level one-liner

“All CASE expressions are evaluated row-wise in a single pass, producing multiple derived columns, and then GROUP BY aggregates each column independently.”




#######################################FINAL CLEAR EXPLANATION #######################################


full intermediate table when all CASE statements are applied together.


query:

MAX(CASE WHEN Subject = 'English' THEN Marks ELSE 0 END) AS English,
MAX(CASE WHEN Subject = 'Maths' THEN Marks ELSE 0 END) AS Maths,
MAX(CASE WHEN Subject = 'Physics' THEN Marks ELSE 0 END) AS Physics,
MAX(CASE WHEN Subject = 'Chemistry' THEN Marks ELSE 0 END) AS Chemistry,
MAX(CASE WHEN Subject = 'Biology' THEN Marks ELSE 0 END) AS Biology

🔹 Step 1: Original Input
| StudentId | Subject   | Marks | Year |
| --------- | --------- | ----- | ---- |
| 1         | English   | 90    | 2024 |
| 1         | Maths     | 91    | 2024 |
| 1         | Physics   | 92    | 2024 |
| 1         | Chemistry | 93    | 2024 |
| 1         | Physics   | 98    | 2025 |
| 1         | Chemistry | 87    | 2025 |
| 1         | English   | 92    | 2025 |
| 1         | Maths     | 95    | 2025 |
| 1         | Biology   | 70    | 2024 |



Step 2: Apply ALL CASE expressions (single pass)
Now the engine evaluates all CASE conditions per row, creating multiple columns.

| StudentId | Year | Subject   | Marks | English_col | Maths_col | Physics_col | Chemistry_col | Biology_col |
| --------- | ---- | --------- | ----- | ----------- | --------- | ----------- | ------------- | ----------- |
| 1         | 2024 | English   | 90    | 90          | 0         | 0           | 0             | 0           |
| 1         | 2024 | Maths     | 91    | 0           | 91        | 0           | 0             | 0           |
| 1         | 2024 | Physics   | 92    | 0           | 0         | 92          | 0             | 0           |
| 1         | 2024 | Chemistry | 93    | 0           | 0         | 0           | 93            | 0           |
| 1         | 2024 | Biology   | 70    | 0           | 0         | 0           | 0             | 70          |
| 1         | 2025 | Physics   | 98    | 0           | 0         | 98          | 0             | 0           |
| 1         | 2025 | Chemistry | 87    | 0           | 0         | 0           | 87            | 0           |
| 1         | 2025 | English   | 92    | 92          | 0         | 0           | 0             | 0           |
| 1         | 2025 | Maths     | 95    | 0           | 95        | 0           | 0             | 0           |


🔹 Step 3: GROUP BY (StudentId, Year)

Now rows are grouped:

Group 1 → (1, 2024)

Rows:

[90,0,0,0,0]
[0,91,0,0,0]
[0,0,92,0,0]
[0,0,0,93,0]
[0,0,0,0,70]


Group 2 → (1, 2025)

Rows:
[0,0,98,0,0]
[0,0,0,87,0]
[92,0,0,0,0]
[0,95,0,0,0]


🔹 Step 4: Apply MAX on each column
For 2024:
English → max(90,0,0,0,0) = 90
Maths → max(0,91,0,0,0) = 91
Physics → max(0,0,92,0,0) = 92
Chemistry → max(0,0,0,93,0) = 93
Biology → max(0,0,0,0,70) = 70

For 2025:
English → 92
Maths → 95
Physics → 98
Chemistry → 87
Biology → 0 (no row → all zeros)

🔹 Final Output

| StudentId | Year | English | Maths | Physics | Chemistry | Biology |
| --------- | ---- | ------- | ----- | ------- | --------- | ------- |
| 1         | 2024 | 90      | 91    | 92      | 93        | 70      |
| 1         | 2025 | 92      | 95    | 98      | 87        | 0       |


🔹 Key Takeaway (this is what interviewers want)
All CASE expressions run together → produce multiple sparse columns →
GROUP BY collapses rows → MAX picks the actual value







"""
# print(__doc__)


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



from pyspark.sql import SparkSession

from pyspark.sql.functions import  *

spark = SparkSession.builder.appName("StudentData").getOrCreate()

data = [
    (1, "English", 90, 2024),
    (1, "Maths", 91, 2024),
    (1, "Physics", 92, 2024),
    (1, "Chemistry", 93, 2024),
    (1, "Physics", 98, 2025),
    (1, "Chemistry", 87, 2025),
    (1, "English", 92, 2025),
    (1, "Maths", 95, 2025),
    (1, "Biology", 70, 2024)
]

columns = ["StudentId", "Subject", "Marks", "Year"]

df = spark.createDataFrame(data, columns)

df.show()

# by using spark sql
print("SPARK SQL")
df.createOrReplaceTempView("student")


spark.sql("""
select StudentId,Year,
COALESCE(max(case when Subject='English' then 'marks' else 0 end ),0) as english,
COALESCE(max(case when Subject='Maths' then 'marks' else 0 end ),0) as Maths,
COALESCE(max(case when Subject='Physics' then 'marks' else 0 end ),0) as Physics,
COALESCE(max(case when Subject='Chemistry' then 'marks' else 0 end ),0) as Chemistry,
COALESCE(max(case when Subject='Biology' then 'marks' else 0 end ),0)  as Biology
from student
group by StudentId,Year
""").show()

print("SPARK DSL")


df.groupby(col("StudentId"),col("Year"))\
    .agg(coalesce( max( when(col("Subject")=='English' ,col('marks')) ) ,lit(0)).alias("English"), \
         coalesce( max( when(col("Subject")=='Maths' ,col('marks')) ) ,lit(0)).alias("Maths"), \
         coalesce( max( when(col("Subject")=='Physics' ,col('marks')) ) ,lit(0)).alias("Physics"),\
         coalesce( max( when(col("Subject")=='Chemistry' ,col('marks')) ) ,lit(0)).alias("Chemistry"), \
         coalesce( max( when(col("Subject")=='Biology' ,col('marks')) ) ,lit(0)).alias("Biology"),
         ).show()

print("by using pivot")
#GPS

df.groupby(col("StudentId"),col("Year"))\
    .pivot("Subject"). agg(sum(col("Marks"))) \
    .na.fill(0).show()


# df.groupBy("StudentId", "Year") \
#     .pivot("Subject") \
#     .agg(sum("Marks")) \
#     .na.fill(0).show()

