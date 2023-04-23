import os
import sys

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from sys import platform
if platform == "darwin":
    os_platform = "Mac"
    clearConsole()
else:
    os_platform = "Windows"
    clearConsole()

# Create spark context
spark = SparkSession.builder.master('local[1]') \
    .appName('SparkByExamples.com') \
    .getOrCreate()

print(spark.sparkContext)
print('Spark App Name: ' + spark.sparkContext.appName)

# Stop PySpark SparkContext
# spark.sparkContext.stop()

# Creating SparkContext prior to PySpark 2.0
# from pyspark import SparkContext, SparkConf
# sc = SparkContext('local', 'Spark_Example_App')
# print(sc.appName)

# Create with getOrCreate
# conf = SparkConf()
# conf.setMaster('local').setAppName('Spark Example App')
# sc = SparkContext.getOrCreate(conf)
# print(sc.appName)

# Create RDD
# rdd = spark.sparkContext.range(1, 5)
# print(rdd.collect())

# Commonly used variables
# print(applicationID)
# print(spark.version)
# print(spark.uiWebUrl)

