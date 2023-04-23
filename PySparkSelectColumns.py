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

data = [('James','Smith','USA','CA'),
        ('Michael','Rose','USA','NY'),
        ('Robert','Williams','USA','CA'),
        ('Maria','Jones','USA','FL')]

columns = ['firstname','lastname','country','state']
df = spark.createDataFrame(data=data, schema=columns)
df.show(truncate=False)

#Select single & multiple columns 
df.select('firstname','lastname').show()
df.select(df.firstname, df.lastname).show()
df.select(df['firstname'], df['lastname']).show()
#Using col function
from pyspark.sql.functions import col
df.select(col('firstname'), col('lastname')).show()
#Using regular expressions
df.select(df.colRegex("`^.*name*`")).show()

#Select all columns
df.select(*columns).show()
df.select([col for col in df.columns]).show()
df.select('*').show()

#Select by index
#Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)
#Selects columns 2 to 4 and top 3 rows
df.select(df.columns[2:4]).show(3)