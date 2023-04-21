import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Default - displays 20 rows and 20 characters from column value
#df.show()

#Display full column contents
#df.show(truncate=False)

#Display 2 rows and full column contents
#df.show(2, truncate=False)

#Display 2 rows and 25 characters in columns
#df.show(2, truncate=25)

#Display dataframe rows and columns vertically
#df.show(n=3,truncate=25,vertical=True)

columns = ['Seqno','Quote']
data = [('1', 'Be the change you wish to see in the world'),
        ('2','Everyone thinks of changing the world, but no one thinks of changing himself'),
        ('3','The purpose of our lives is to be happy'),
        ('4','Be cool')]

df = spark.createDataFrame(data,columns)
#Default show()
df.show()

#Display full columns
df.show(truncate=False)

