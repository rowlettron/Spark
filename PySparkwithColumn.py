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

data  = [('James','','Smith','1991-04-01','M',3000),
    ('Michael','Rose','','2000-05-19','M',4000),
    ('Robert','','Williams','1978-09-05','M',4000), 
    ('Maria','Anne','Jones','1967-12-01','F',4000),
    ('Jen','Mary','Brown','1980-01-17','F',-1)
    ]

columns = ['firstname','middlename','lastname','dob','gender','salary']

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn('salary',col('salary').cast('Integer'))
df2.printSchema()
df2.show(truncate=False)

df3 = df.withColumn('salary',col('salary')*100)
df3.printSchema()
df3.show(truncate=False)

df4 = df.withColumn('CopiedColumn', col('salary') * -1)
df4.printSchema()
df4.show(truncate=False)

df5 = df.withColumn('Country', lit('USA'))
df5.printSchema()
df5.show(truncate=False)

df6 = df.withColumn('Country',lit('USA')) \
    .withColumn('anotherColumn', lit('anotherColumn'))
df6.printSchema()
df6.show(truncate=False)

df.withColumnRenamed('gender', 'sex').show(truncate=False)

df4.drop('copiedColumn').show(truncate=False)
