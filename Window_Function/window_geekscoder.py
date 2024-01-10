# Sample Window funcions
# row_number(),dense_rank()

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

data = [(1, "Sagar", "CSE", "UP", 80),
        (2, "Shivam", "IT", "MP", 86),
        (3, "Muni", "Mech", "Ap", 70),
        (1, "Sagar", "CSE", "UP", 80),
        (3, "Muni", "Mech", "Ap", 70)]
schema = ["id", "student_name", "department", "city", "marks"]

spark = SparkSession.builder.appName("window function geeks coder").getOrCreate()

df = spark.createDataFrame(data, schema)

df.show()

row_df = df.withColumn("row_number", row_number().over(Window.partitionBy("id").orderBy("marks")))

row_df.show()

# rank function will assign same rank if that rows have exact same value

rank_df = df.withColumn("rank_number", rank().over(Window.partitionBy("id").orderBy("marks")))

rank_df.show()

# dense rank

dense_rank_df = df.withColumn("denserank", dense_rank().over(Window.partitionBy("id", "department").orderBy("marks")))

dense_rank_df.show()


