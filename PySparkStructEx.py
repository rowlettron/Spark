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
from pyspark.sql.functions import col, struct, when

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

# Using PySpark StructType & StructField with DataFrame
data = {('James','','Smith','36636','M',3000),
        ('Michael','Rose','','40288','M',4000),
        ('Robert','','Williams','42114','M',4000),
        ('Maria','Anne','Jones','39192','F',4000),
        ('Jen','Mary','Brown','','F',-1)}

schema = StructType([ \
    StructField('firstname', StringType(), True), \
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True),
    StructField('id', StringType(), True),
    StructField('gender',StringType(), True),
    StructField('salary', StringType(), True)
    ])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

# Defining neste StructType object struct
structureData = {(('James','','Smith'),'36636','M',3100),
        (('Michael','Rose',''),'40288','M',4300),
        (('Robert','','Williams'),'42114','M',1000),
        (('Maria','Anne','Jones'),'39192','F',5500),
        (('Jen','Mary','Brown'),'','F',-1)}

structureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])), 
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df2 = spark.createDataFrame(data=structureData, schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)

# Adding & Changing struct of the data frame
updatedDF = df2.withColumn('OtherInfo', struct(col('id').alias('identifier'),
            col('gender').alias('gender'),
            col('salary').alias('salary'),
            when(col('salary').cast(IntegerType()) < 2000 , 'Low')
                .when(col('salary').cast(IntegerType()) < 4000, 'Medium')
                .otherwise('High').alias('Salary_Grade')
                )).drop('id','gender','salary')

updatedDF.printSchema()
updatedDF.show(truncate=False)

# Using SQL ArrayType and MapType
arrayStructureSchema = StructType([
    StructField('name', StructType([
       StructField('firstname', StringType(), True),
       StructField('middlename', StringType(), True),
       StructField('lastname', StringType(), True)
       ])),
       StructField('hobbies', ArrayType(StringType()), True),
       StructField('properties', MapType(StringType(),StringType()), True)
    ])