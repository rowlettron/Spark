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
from pyspark.sql.functions import col, struct, when, lit, expr, sum, avg, max, min, mean, count
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

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

#Inner join
empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id, 'inner').show(truncate=False)

#Full outer join
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'outer').show(truncate=False)
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'full').show(truncate=False)
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'fullouter').show(truncate=False)

#Left outer join
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'left').show(truncate=False)
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'leftouter').show(truncate=False)

#Right outer join
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'right').show(truncate=False)
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'rightouter').show(truncate=False)

#Left semi join
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'leftsemi').show(truncate=False)
