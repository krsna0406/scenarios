"""

scenario 15 : Extend and Append list in python and scala
input:

expected :
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
l1 = [2, 3, 4, 5]
l2 = [6, 7, 8, 9]
# append
# appendlst = l1.append(l2)
# print(l1)

# extend
l1.extend(l2)
print(l1)