#https://spark.apache.org/docs/latest/sql-data-sources-text.html

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DoubleType,DataType
from utility import *

spark = SparkSession.builder.appName('txt').master("local").getOrCreate()
print(spark)

# A text dataset is pointed to by path.
# The path can be either a single text file or a directory of text files
path = "Data/Dataset.txt"

df1 = spark.read.text(path)
df1.show()

# You can use 'lineSep' option to define the line separator.
# The line separator handles all `\r`, `\r\n` and `\n` by default.
df2 = spark.read.text(path, lineSep=";")
df2.show()

# You can also use 'wholetext' option to read each input file as a single row.
df3 = spark.read.text(path, wholetext=True)
df3.show()
# "output" is a folder which contains multiple text files and a _SUCCESS file.
df1.write.csv("output")

# You can specify the compression format using the 'compression' option.
df1.write.text("output_compressed", compression="gzip")
