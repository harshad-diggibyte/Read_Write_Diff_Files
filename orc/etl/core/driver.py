#https://spark.apache.org/docs/latest/sql-data-sources-orc.html

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DoubleType,DataType
from utility import *

spark = SparkSession.builder.appName('orc').master("local").getOrCreate()
print(spark)

#Reading .orc file

df = spark.read.format('orc').load('Data/orcfile.orc')
df.show()
df.printSchema()

#Writing .orc file
df.write.mode('overwrite').format('orc').save('Output/orc_file')
