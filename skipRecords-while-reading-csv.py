#Skip Specific Range of Records while Reading CSV File

##Input CSV file                                                            
#Data                                                        #DataFrame
#1                                                                  #1
#2                                                                  #2
#3                                                                  #7
#4                                                                  
#5                                                                  
#6                                                                 
#7                                                                 


%fs
ls /FileStore/tables/

#dbutils.fs.ls("/FileStore/tables/")

#Create Dataframe
df=(spark.read.format("csv")\
   .option("inferSchema",True)\
   .option("header",True)\
   .option("sep",",")\
   .load("/FileStore/tables/baby_names.csv"))

display(df)

#Skip starting rows of specific range
skipStartdf=(spark.read.format("csv")
   .option("inferSchema",True)
   .option("header",True)
   .option("sep",",")
   .option("skipRows",10)
   .load("/FileStore/tables/baby_names.csv"))

display(skipStartdf)

#Skip Ending row of specific range
skipEnddf=(spark.read.format("csv")
   .option("inferSchema",True)
   .option("header",True)
   .option("sep",",")
   .option("skipRows",20)
   .load("/FileStore/tables/baby_names.csv"))

display(skipEnddf)

#Subtract skipStartdf  from main dataframe  
deltadf=df.subtract(skipStartdf)
display(deltadf.orderBy("Id"))

#Combining Delta and Skip dataframe 
finalDf=deltadf.union(skipEnddf)
display(finalDf.orderBy("Id"))




###############################################################
#UDF
def skipRowsRangeCSV(filePath,startPos,endPos):
  df=(spark.read.format("csv")\
     .option("inferSchema",True)\
     .option("header",True)\
     .option("sep",",")\
     .load(filePath))
  
  skipStartdf=(spark.read.format("csv")
     .option("inferSchema",True)
     .option("header",True)
     .option("sep",",")
     .option("skipRows",startPos)
     .load(filePath))
     
     
  skipEnddf=(spark.read.format("csv")
     .option("inferSchema",True)
     .option("header",True)
     .option("sep",",")
     .option("skipRows",endPos)
     .load(filePath))

  deltadf=df.subtract(skipStartdf)
  finalDf=deltadf.union(skipEnddf)
  
  return finalDf
  
outputDF=skipRowsRangeCSV(filePath="dbfs:/FileStore/tables/baby_names.csv",startPos=10,endPos=20)
display(outputDF.orderBy("Id"))
