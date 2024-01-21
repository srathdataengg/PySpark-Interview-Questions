""""
find the actors and directors who have worked at least 3 times
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, lit, col
from pyspark.sql.types import StructType, StructField, IntegerType

data = [(1, 1, 0),
        (1, 1, 1),
        (1, 1, 2),
        (1, 2, 3),
        (1, 2, 4),
        (2, 1, 5),
        (2, 1, 6)]

schema = StructType([
    StructField("ActorID", IntegerType(), True),
    StructField("DirectorID", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])

spark = SparkSession.builder.appName("Question 1").getOrCreate()
df = spark.createDataFrame(data, schema=schema)

df.show()

resultdf = df.groupby("actorid", "directorid").agg(count(lit("1")).alias("row_count")).filter(col("row_count")>=3)

resultdf.show()
