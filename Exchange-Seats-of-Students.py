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


#################SPARK SQL##################
df.createOrReplaceTempView("df")
select * from df;

--Exchange seat for the first 2 student----
select id,name as original_seating,
case when id % 2=1 THEN coalesce(lead(name) over (order by id),name)
        when id % 2=0 THEN coalesce(lag(name) over (order by id),name)
		else name
end as exchanged_seating
from df;

--Exchange seat for the first 2 student----

select id,name as original_seating,
case when id=1 THEN lead(name) over (order by id)
     when id=2 THEN lag(name) over (order by id)
else
name
end as exchanged_seating
from df;


--Without window function--- 

select s1.*,s1.id,s2.id-1 ,s3.id+1,
s2.name as next,s3.name as prev 
from df s1 
left join df s2 on s1.id=s2.id-1 
left join df s3 on s1.id=s3.id+1
