#https://spark.apache.org/docs/latest/sql-data-sources-json.html

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DoubleType,DataType
from utility import *

spark = SparkSession.builder.appName('txt').master("local").getOrCreate()
print(spark)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files
#path = "Data/sample1.json"


#Creating dataframe using Simple json file

df= spark.read.format('json').options(inferSchema='True',multiline= 'True').load('Data/sample1.json')
print("dataframe: df")
df.show()
df.printSchema()

df.write.format('json').mode('overwrite').save(path='/Output/result_json_s')

#Creating dataframe using multiline json file

df1= spark.read.format('json').options(inferSchema='True', multiline='True').load('Data/sample1.json')
print("dataframe: df1")
df1.show(truncate= False)
df1.printSchema()

#writing dataframe to json file
print('writing multiline json file')
df1.write.format('json').mode('overwrite').save(path='Output/result_json_m')
