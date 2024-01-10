# Spark Interview Questions
# Read the third quarter (25%) of a file in PySpark

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

data = [("Alice", 28),
        ("Bob", 35),
        ("Charlie", 42),
        ("David", 25),
        ("Eva", 31),
        ("Frank", 38),
        ("Grace", 45),
        ("Henry", 29)
        ]
schema = StructType([StructField("name", StringType(), True),
                     StructField("age", IntegerType(), True)])
spark = SparkSession.builder.appName("Interview Question-Lipsa Biswas").getOrCreate()
df = spark.createDataFrame(data=data, schema=schema)

df = df.withColumn("order_col", F.monotonically_increasing_id())

df.show(truncate=False)
