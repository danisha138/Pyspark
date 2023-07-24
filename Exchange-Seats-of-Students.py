df=spark.read.format("csv").option('header','True').option('del',',').option('inferSchema','True').load('dbfs:/FileStore/tables/student.csv')
display(df)

from pyspark.sql.window import Window
from pyspark.sql.functions import *

exchangeDF=df.withColumn('prev_student', lag('name').over(Window.orderBy('id')))
exchangeDF=exchangeDF.withColumn('next_student', lead('name').over(Window.orderBy('id')))

display(exchangeDF)

exchangeDF=exchangeDF.withColumn('exchanged_seating', 
                                 when (exchangeDF['id'] %2 ==1 ,coalesce(exchangeDF['next_student'],exchangeDF['name']))
                                 .when (exchangeDF['id'] %2 ==0 ,coalesce(exchangeDF['prev_student'],exchangeDF['name']))
                                 .otherwise(exchangeDF['name']))
exchangeDF=exchangeDF.withColumnRenamed("name","original_seating").drop("prev_student","next_student")
exchangeDF.display()

exchangeDF=exchangeDF.drop("original_seating").withColumnRenamed("exchanged_seating","student")

exchangeDF.show()
