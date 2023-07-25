from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

spark = SparkSession.builder.appName("MeanValueExample").getOrCreate()

data = [
  (1, "Soumya", 22),
  (2, "Subham", None),
  (3, "Sangram", 30),
  (4, "Dinesh", 28),
  (5, "Santosh", None)
]

schema = ["id", "name", "age"]
df = spark.createDataFrame(data, schema)

mean_val = df.select(mean(df["age"])).collect()
mean_value = mean_val[0][0]

df_filled_with_mean = df.na.fill(mean_value, subset=["age"])

df_filled_with_mean.show()
