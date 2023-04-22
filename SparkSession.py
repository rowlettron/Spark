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

# Create SparkSession
# spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()

# Create another SparkSession
spark2 = SparkSession.newSession
# print(spark2)

# Get existing SparkSession
spark3 = SparkSession.builder.getOrCreate()
# print(spark3)

# Using Spark Config
spark = SparkSession.builder \
    .master('local[1]') \
    .appName('SparkByExamples.com') \
    .config('spark.come.config.option', 'config-value') \
    .getOrCreate()

# Create SparkSession with Hive enable
spark = SparkSession.builder \
    .master('local[1]') \
    .appName('SparkByExamples.com') \
    .config('spark.sql.warehouse.dir', '<path>/spark-warehouse') \
    .enableHiveSupport() \
    .getOrCreate()

# print(spark)

# Using PySpark Configs
spark.conf.set('saprk.sql.shuffle.partitions', '5')

partitions = spark.conf.get('saprk.sql.shuffle.partitions')
# print(partitions)

# Create PySpark DataFrame
df = spark.createDataFrame(
    [('Scala', 25000), ('Spark', 35000), ('PHP', 21000)]
)

# df.show()

# Working with Spark SQL
# df.createOrReplaceTempView('sample_table')
# df2 = spark.sql('SELECT _1, _2 FROM sample_table')
# df2.show()

# Create Hive table
# spark.table('sample_table').write.saveAsTable('sample_hive_table')
# df3 = spark.sql('select _1, _2 from sample_hive_table')
# df3.show()

# Working with catalogs
dbs = spark.catalog.listDatabases()
print(dbs)

# List tables
tbls = spark.catalog.listTables()
print(tbls)




