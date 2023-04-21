from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Create empty RDD
emptyRDD = spark.sparkContext.emptyRDD()
print(emptyRDD) 

