###The [^0-9] expression is used to find any character that is NOT a digit.
###replace all occurrences of the dollar sign character "$" in the "Price" column with an empty string

##Create Dataframe
df=spark.read.load('/FileStore/tables/googleplaystore.csv',format='csv',sep=',',header='true',escape='"',inferSchema='true')


#Cleaning of data and Datatype 
from pyspark.sql.functions import regexp_replace,col

df=df.withColumn("Reviews",col("Reviews").cast(IntegerType()))\
     .withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
     .withColumn("Installs",col("Installs").cast(IntegerType()))\
     .withColumn("Price",regexp_replace(col("Price"),"[$]",""))\
     .withColumn("Price",col("Price").cast(IntegerType()))
