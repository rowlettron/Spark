import os
import sys
import pyspark

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def clearConsole():
    if os.name in ('nt','dos'):
        command = 'cls'
    else:
        command = 'clear'

    os.system(command)
    
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import col, struct, when, lit
from pyspark.sql import Row

from sys import platform
if platform == "darwin":
    os_platform = "Mac"
    clearConsole()
else:
    os_platform = "Windows"
    clearConsole()

spark = SparkSession.builder.master('local[1]') \
    .appName('SparkByExamples.com') \
    .getOrCreate()

dept = [('Finance',10), ('Marketing', 20), ('Sales', 30), ('IT', 40)]
deptColumns = ['dept_name','dept_id']
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
deptDF.show(truncate=False)

#Use collect() to retrieve the data
dataCollect = deptDF.collect()
print(dataCollect)

for row in dataCollect:
    print(row['dept_name'] + ', ' + str(row['dept_id']))

#get first row and first column
print(deptDF.collect()[0][0])

#return certain elements of a data frame
dataCollect = deptDF.select('dept_name').collect()
print('')
print(dataCollect)



