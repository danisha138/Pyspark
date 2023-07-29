import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *


#Create Dataframe
df=spark.read.load('/FileStore/tables/loan.csv',format='csv',sep=',',header='true',escape='"',inferSchema='true')

# Check the count of loaded file
df.count()

#Check the loaded data of df
df.show(2)

#Print the Schema
df.printSchema()

#Change the Datatype and Cleaning of data(remove the comma(,) from column Debt)
from pyspark.sql.functions import regexp_replace,col
df = df.withColumn("Debt",regexp_replace(col("Debt"),"[,]",""))\
       .withColumn("Debt",col("Debt").cast("Int"))



df.show(2)
