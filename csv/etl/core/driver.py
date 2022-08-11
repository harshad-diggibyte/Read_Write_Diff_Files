#https://spark.apache.org/docs/latest/sql-data-sources-csv.html

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,TimestampType,DoubleType,DataType
from utility import *

spark = SparkSession.builder.appName('csv').master("local").getOrCreate()
print(spark)

schema_defined= StructType([StructField('Username', StringType(), True),
                            StructField('Identifier', IntegerType(), True),
                            StructField('Recovery code', StringType(),True),
                            StructField('First name', StringType(),True),
                            StructField('Last name', StringType(),True),
                            StructField('Department', StringType(),True),
                            StructField('Location', StringType(),True),
                            ])

df= spark.read.format("csv").schema(schema_defined).option("header",True).option("delimiter",";").load("Data/username-password-recovery-code.csv")
df.printSchema()
df.show()

df.write.mode('overwrite').csv("Data_csv/create_csv")


#MODE OPTIONS
"""
overwrite – mode is used to overwrite the existing file.
append – To add the data to the existing file.
ignore – Ignores write operation when the file already exists.
error – This is a default option when the file already exists, it returns an error.
"""
"""
df.write.mode('append').csv("Data_csv/create_csv")
df.write.mode('ignore').csv("Data_csv/create_csv")
df.write.mode('error').csv("Data_csv/create_csv")
"""
"""
from pyspark.sql.functions import date_add,to_date,col,expr,to_timestamp 
schema_define= StructType([StructField('InvoiceNo', IntegerType(), True),
                            StructField('StockCode', StringType(), True),
                            StructField('Description', StringType(),True),
                            StructField('Quantity', IntegerType(),True),
                            StructField('InvoiceDate', TimestampType(),True),
                            StructField('UnitPrice', DoubleType(),True),
                            StructField('CustomerID', IntegerType(),True),
                            StructField('Country', StringType(),True),
                            ])
"""
df1= spark.read.format("csv").option("header",True).option("delimiter",",").load("Data/OnlineRetail.csv")
df1.printSchema()
df1.show()

"""
PERMISSIVE: when it meets a corrupted record, puts the malformed string into a field configured by columnNameOfCorruptRecord, and sets malformed fields to null. 
To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. 
If a schema does not have the field, it drops corrupt records during parsing. 
A record with less/more tokens than schema is not a corrupted record to CSV. 
When it meets a record having fewer tokens than the length of the schema, sets null to extra fields. 
When the record has more tokens than the length of the schema, it drops extra tokens.
DROPMALFORMED: ignores the whole corrupted records. This mode is unsupported in the CSV built-in functions.
FAILFAST: throws an exception when it meets corrupted records.
"""


#Permissive Mode
df2= spark.read.format("csv").option("mode", "PERMISSIVE").option("header", "true").load("Data/OnlineRetail.csv")
"""
#DropMalformed Mode
df = spark.read.format("csv").option("mode","DROPMALFORMED").option("header","true").schema(schema).load("Data/OnlineRetail.csv")
#Fail Fast Mode
df=spark.read.format("csv").option("mode","FAILFAST").option("header","true").schema (schema).load("Data/OnlineRetail.csv")
display(df)
"""
