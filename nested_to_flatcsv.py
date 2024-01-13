"""
Consider a PySpark DataFrame with a column named nested_data containing nested structures (StructType) in the following format:

root
 |-- id: integer
 |-- details: struct
 |    |-- name: string
 |    |-- age: integer
 |    |-- address: struct
 |    |    |-- city: string
 |    |    |-- country: string
Your task is to perform the following operations:

1)Extract the name and age fields from the details struct.
2)Create a new column named full_address by concatenating the city and country fields from the nested address struct.
Write PySpark code to achieve these tasks and provide the resulting DataFrame schema.

Channel - Pysparkpulse
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [(1, ("John", 25, ("NewYork", "USA"))),
        (2, ("Alice", 30, ("London", "UK"))),
        (3, ("Bob", 28, ("Sydney", "Australia"))),
        (4, ("Soumya", 35, ("Bangalore", "india")))]
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("details", StructType([
        StructField("name", StringType(), True),
        StructField("age", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]), True)

    ]), True)
])

spark = SparkSession.builder.appName("nested schema").getOrCreate()
nested_df = spark.createDataFrame(data=data, schema=schema)

nested_df.show(truncate=False)
nested_df.printSchema()

nested_df1 = nested_df.withColumn("name", nested_df["details.name"]) \
    .withColumn("age", nested_df["details.age"]) \
    .withColumn("address", concat_ws(" ",nested_df["details.address.city"], nested_df["details.address.country"]))\
    .drop("details")

nested_df1.show(truncate=False)
