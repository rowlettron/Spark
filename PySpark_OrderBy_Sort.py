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



simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
# df.printSchema()
# df.show(truncate=False)

#Sorting with the sort function
# df.sort("department","state").show(truncate=False)
# df.sort(col('department'), col('state')).show(truncate=False)

#Sorting with the orderby function
# df.orderBy('department','state').show(truncate=False)
# df.orderBy(col('department'), col('state')).show(truncate=False)

#Sort ascending
# df.sort(df.department.asc(), df.state.asc()).show(truncate=False)
# df.sort(col('department').asc(), col('state').asc()).show(truncate=False)
# df.orderBy(col('department').asc(), col('state').asc()).show(truncate=False)

#Sort descending
# df.sort(df.department.asc(), df.state.desc()).show(truncate=False)
# df.sort(col('department').asc(), col('state').desc()).show(truncate=False)
# df.orderBy(col('department').asc(), col('state').desc()).show(truncate=False)

#Using raw sql
df.createOrReplaceTempView('EMP')
spark.sql('select employee_name, department, state, salary, age, bonus from EMP order by department asc').show(truncate=False)

