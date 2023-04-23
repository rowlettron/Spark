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

#Create Column Class Object
colObj = lit('SparkByExamples.com')
data = [('James',23), ('Anne', 40)]
df = spark.createDataFrame(data).toDF('name.fname', 'gender')
df.printSchema()

# Using DataFrame object (df)
df.select(df.gender).show()
df.select(df['gender']).show()
#Accessing column name with dot (with backticks)
df.select(df['`name.fname`']).show()

#Using SQL col() function
from pyspark.sql.functions import col
df.select(col('gender')).show()
#Accessing column name with dot (with backticks)
df.select(col('`name.fname`')).show()

#Accessing struct type columns
#Create DataFrame with struct using row class
from pyspark.sql import Row
data = [Row(name='James', prop=Row(hair='black',eye='blue')),
        Row(name='Anne', prop=Row(hair='grey', eye='black'))]

df = spark.createDataFrame(data)
df.printSchema()

#Access struct column
df.select(df.prop.hair).show()
df.select(df['prop.hair']).show()
df.select(col('prop.hair')).show()

#PySpark column operators
data = [(100,2,1), (200,3,4), (300,4,4)]
df = spark.createDataFrame(data).toDF('col1','col2','col3')
# Arthmetic operations
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()

df.select(df.col2 > df.col3).show()
df.select(df.col2 < df.col3).show()
df.select(df.col2 == df.col3).show()

#PySpark Column Functions
data = [('James','Bond','100',None),
        ('Ann','Varsa','200', 'F'),
        ('Tom Cruise','XXX','400',''),
        ('Tom Brand', None, '400', 'M')]
columns = ['fname','lname','id','gender']
df = spark.createDataFrame(data, columns)

# ailias() - Sets name to column
from pyspark.sql.functions import expr
df.select(df.fname.alias('first_name'), \
          df.lname.alias('last_name')
).show()

df.select(expr(" fname ||','|| lname").alias('fullName')).show()

#asc() & desc() - Sort columns
df.sort(df.fname.asc()).show()
df.sort(df.fname.desc()).show()

#cast() & astype() - used to convert data type
df.select(df.fname, df.id.cast('int')).printSchema()


