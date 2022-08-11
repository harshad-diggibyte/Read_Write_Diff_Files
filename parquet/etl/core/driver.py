#https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DoubleType,DataType
from utility import *

spark = SparkSession.builder.appName('parquet').master("local").getOrCreate()
print(spark)


DF = spark.read.format("csv").option("header",True).option("delimiter",";").load("Data/smallwikipedia.csv")
DF.show()
# DataFrames can be saved as Parquet files, maintaining the schema information.
DF.write.mode('overwrite').parquet("Data/smallwikipedia.parquet")

# Read in the Parquet file created above.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
df1 = spark.read.format("parquet").option("header",True).option("delimiter",";").load("Data/smallwikipedia.parquet")
df1.show()
df1.printSchema()

# Parquet files can also be used to create a temporary view and then used in SQL statements.
df1.createOrReplaceTempView("df1")
change = spark.sql("SELECT changes FROM df1 WHERE changes >= 20")
change.show()

