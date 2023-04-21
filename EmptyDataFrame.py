# Create Schema
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Create empty RDD
emptyRDD = spark.sparkContext.emptyRDD()

# Create empty dataframe with schema
schema = StructType([ 
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True)
])

df = spark.createDataFrame(emptyRDD, schema)
df.printSchema()

# convert empty RDD to dataframe
df1 = emptyRDD.toDF(schema)
df1.printSchema()

# create empty dataframe with schema
df2 = spark.createDataFrame([], schema)
df2.printSchema()

#create empty dataframe without schema (no columns)
df3 = spark.createDataFrame([], StructType([]))
df3.printSchema()

