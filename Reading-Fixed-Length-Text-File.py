%fs put /FileStore/city_temp.txt """20200313MUM40000129.5
20200314MUM40000129.6
20200315MUM40000121.4 """

#Review sample data
%fs head /FileStore/city_temp.txt

#Read Text File
df=spark.read.format("text").load("/FileStore/city_temp.txt")

#spark.read.read("/FileStore/city_temp.txt")

display(df)


#Define Schema and field positions using a dictionary
fieldDetails={"Date":(1,8),"CityCode":(9,3),"PinCode":(12,6),"Temperature":(18,4)}


#Splitting the fixed length text into columns
from pyspark.sql.functions import substring

for field ,(start,length) in fieldDetails.items():
    df=df.withColumn(field,substring(df["value"],start,length))

df=df.drop("value")

df.display();


+--------+--------+-------+-----------+
|    Date|CityCode|PinCode|Temperature|
+--------+--------+-------+-----------+
|20200313|     MUM| 400001|       29.5|
|20200314|     MUM| 400001|       29.6|
|20200315|     MUM| 400001|       21.4|
+--------+--------+-------+-----------+


