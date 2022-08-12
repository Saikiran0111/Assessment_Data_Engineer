#Dataset-1
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()
store_df1 = spark.read.csv("USA.csv")
Schema=StructType([ StructField("ID",IntegerType(),nullable=True),
 StructField("Name",StringType(),nullable=True),
 StructField("VaccinationType",StringType(),nullable=True),
 StructField("VaccinationDate",StringType(),nullable=True),
StructField("Region",StringType(),nullable=False),
])
df1 = spark.read.option("header",True).schema(Schema).csv("USA.csv")
df1=df1.na.fill(value="USA",subset=["Region"])
df1.show()

#Dataset-2
from pyspark.sql.functions import col, to_date
store_df2 = spark.read.csv("AUS.csv")
Schema=StructType([ StructField("ID",IntegerType(),nullable=True),
 StructField("Name",StringType(),nullable=True),
 StructField("VaccinationType",StringType(),nullable=True),
StructField("DateOfBirth",StringType(),nullable=True),
 StructField("VaccinationDate",StringType(),nullable=True),
StructField("Region",StringType(),nullable=False),
])
df2 = spark.read.option("header",True).schema(Schema).csv("AUS.csv")
df2 = df2.withColumn('VaccinationDate',to_date(col('VaccinationDate'), 'dd-MM-yyyy
df2 = df2.withColumn('DateOfBirth',to_date(col('DateOfBirth'), 'dd-MM-yyyy'))
df2=df2.na.fill(value="AUS",subset=["Region"])
df2.show()


#Dataset-3
store_df3 = spark.read.csv("IND.csv")
Schema=StructType([ StructField("ID",IntegerType(),nullable=True),
 StructField("Name",StringType(),nullable=True),
StructField("DateOfBirth",StringType(),nullable=True),
 StructField("VaccinationType",StringType(),nullable=True),
 StructField("VaccinationDate",StringType(),nullable=True),
StructField("Free/Paid",StringType(),nullable=True),
StructField("Region",StringType(),nullable=False),
])
df3 = spark.read.option("header",True).schema(Schema).csv("IND.csv")
df3 = df3.withColumn('VaccinationDate',to_date(col('VaccinationDate'), 'yyyy-MM-dd
df3 = df3.withColumn('DateOfBirth',to_date(col('DateOfBirth'), 'yyyy-MM-dd'))
df3=df3.na.fill(value="INDIA",subset=["Region"])
df3.show()



#We are merging all DateFrames stored
import functools
def unionAll(dfs):
 return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), df
unioned_df = unionAll([df1, df2, df3])
unioned_df.show()


#To get Count of people by region
unioned_df.groupBy("Region").count().show(truncate=False)


#To get Count of Vaccination Types
unioned_df.groupBy("VaccinationType").count().show(truncate=False)
