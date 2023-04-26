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
from pyspark.sql.functions import col, struct, when, lit, expr
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

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]

# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
# df.printSchema()
# df.show(truncate=False)

#Get distinct rows
distinctDF = df.distinct()
# print('Distinct count: ' + str(distinctDF.count()))
# distinctDF.show(truncate=False)

df2 = df.dropDuplicates()
print('Distinct count: ' + str(df2.count()))
df2.show(truncate=False)


#PySpark distinct of selected multiple columns
dropDisDF = df.dropDuplicates(['department','salary'])
print('Distinct count of department and salary: ' + str(dropDisDF.count()))
dropDisDF.show(truncate=False)

