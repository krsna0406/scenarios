"""
Table1
	SNo	Cust_name	Account_type	Balances
	1		    a	Savings		    100
	2		    a	Loan		    200
	3		    a	Current		    300
	4		    b	Savings		    200
	5		    b	Loan		    200
	6		    c	Savings		    150
	7		    c	Savings		    240
	8		    d	Loan		    300
	9		    e	Loan		    430
	10		    e	Loan		    300

	a) Fetch the customers who are having only savings account		ans: c
        SELECT Cust_name
        FROM accounts
        GROUP BY Cust_name
        HAVING  COUNT(DISTINCT Account_type) = 1     AND MAX(Account_type) = 'Savings';

        :: explanation on max()
        https://chatgpt.com/c/69d6099a-7644-8322-a4bf-48d8d4c83910
            🔴 Query
                SELECT Cust_name
                FROM accounts
                GROUP BY Cust_name
                HAVING COUNT(DISTINCT Account_type) = 1
                   AND MAX(Account_type) = 'Savings';
                🧾 Example data
                Cust_name | Account_type
                -------------------------
                A         | Savings
                A         | Savings
                B         | Savings
                B         | Current
                C         | Current
                D         | Savings
                🔍 Step 1: GROUP BY Cust_name

                Groups become:

                A
                Savings, Savings
                B
                Savings, Current
                C
                Current
                D
                Savings
                🔍 Step 2: COUNT(DISTINCT Account_type) = 1

                👉 Keeps only customers with only one unique account type

                Customer	Distinct Types	Pass?
                A	Savings	✅
                B	Savings,Current	❌
                C	Current	✅
                D	Savings	✅

                Remaining: A, C, D

                🔍 Step 3: MAX(Account_type) = 'Savings'

                Now apply:

                A
                MAX(Savings, Savings) → Savings ✅
                C
                MAX(Current) → Current ❌
                D
                MAX(Savings) → Savings ✅
                ✅ Final Output
                Cust_name
                ----------
                A
                D
                🧠 Why MAX() is used here
                Key idea:

                After ensuring:

                COUNT(DISTINCT Account_type) = 1

                👉 Each group has only one unique value

                So:

                MAX(Account_type) = that only value
                MIN(Account_type) = same value
                🔵 So this condition:
                MAX(Account_type) = 'Savings'

                Means:

                “That single account type must be Savings”

                ⚡ Equivalent logic (important)

                This is same as:

                HAVING COUNT(DISTINCT Account_type) = 1
                   AND MIN(Account_type) = 'Savings'
                🔥 Why not just use WHERE Account_type = 'Savings'?

                Because that would:

                Filter rows before grouping
                Give wrong result

                Example:

                B → Savings, Current

                If filtered:

                B → Savings only

                👉 Now B incorrectly appears as valid

                🎯 Final interpretation

                Your query finds:

                Customers who have only one type of account AND that type is Savings

                🧠 Interview insight

                Pattern:

                COUNT(DISTINCT col) = 1
                AND MAX(col) = 'X'

                👉 Means:

                “All values in group are X”




	b) Fetch the customers who are having account of single type						ans: c,d,e
        SELECT Cust_name
        FROM accounts
        GROUP BY Cust_name
        HAVING COUNT(DISTINCT Account_type) = 1;
	c) Fetch customers whose loan balance is greater than savings balance						    ans: a,d,e
        SELECT Cust_name
        FROM accounts
        GROUP BY Cust_name
        HAVING
            SUM(CASE WHEN Account_type = 'Loan' THEN Balances ELSE 0 END) > SUM(CASE WHEN Account_type = 'Savings' THEN Balances ELSE 0 END);

For clarity:
COUNT(DISTINCT Account_type)
Counts how many different account types a customer has.
Examples:
Customer	Account Types	        COUNT(DISTINCT)
    a	    Savings, Loan, Current	3
    b	    Savings, Loan	        2
    c	    Savings	                1
    d	    Loan	                1
    e	    Loan	                1



imp note:


"""
# print(__doc__)

import sys
import os
import urllib.request
import ssl

from pyspark.sql.types import IntegerType, DoubleType, StructField, StructType, StringType

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['HADOOP_HOME'] = r'C:\app\zeyoplus\soft\sw\hadoop'
os.environ['JAVA_HOME'] = r'C:\Users\Krishna\.jdks\ms-17.0.16'        #  <----- 🔴JAVA PATH🔴
######################🔴🔴🔴################################

from pyspark.sql import SparkSession


# Initialize Spark
spark = SparkSession.builder.appName("faq").getOrCreate()

