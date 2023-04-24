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

data = [
    (('James','','Smith'),['Java','Scala','C++'], 'OH','M'),
    (('Anna','Rose',''),['Spark','Java','C++'],'NY','F'),
    (('Julia','','Williams'),['CSharp','VB'],'OH','F'),
    (('Maria','Anne','Jones'),['CSharp','VB'],'NY','M'),
    (('Jen','Mary','Brown'),['CSharp','VB'],'NY','M'),
    (('Mike','Mary','Williams'),['Python','VB'],'OH','M')
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

#Filter() with column condition
df.filter(df.state == 'OH').show(truncate=False)

df.filter(df.state != 'OH').show(truncate=False)

df.filter(~(df.state == 'OH')).show(truncate=False)

print('***********************************************************')

#Filter with SQL col() function
df.filter(col('state') == 'OH').show(truncate=False)
print('***********************************************************')

# Filter with SQL expression
df.filter("gender == 'M'").show(truncate=False)
df.filter("gender != 'M'").show(truncate=False)
df.filter("gender <> 'M'").show(truncate=False)

print('***********************************************************')

# Filter with multiple conditions
df.filter((df.state == 'OH') & (df.gender == 'M')).show(truncate=False)
print('***********************************************************')

#Filter based on list values
li = ['OH','CA','DE']
df.filter(df.state.isin(li)).show(truncate=False)
df.filter(~df.state.isin(li)).show(truncate=False)
df.filter(df.state.isin(li) == False).show(truncate=False)

print('***********************************************************')

#Filter based on Starts With, Ends With, and Contains
#Starts with
df.filter(df.state.startswith('N')).show(truncate=False)
#Ends with
df.filter(df.state.endswith('H')).show(truncate=False)
#Contains
df.filter(df.state.contains('H')).show(truncate=False)

print('***********************************************************')

#Filter with like and rlike
data2 = [(2,'Michael Rose'), (3,'Robert Williams'), (4,'Rames Rose'), (5,'Rames rose')]
df2 = spark.createDataFrame(data=data2, schema=['id','name'])

df2.filter(df2.name.like('%rose%')).show(truncate=False)

df2.filter(df2.name.rlike('(?i)^*rose$')).show(truncate=False)