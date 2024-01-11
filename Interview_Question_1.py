# Spark Interview Questions
# Read the third quarter (25%) of a file in PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, ntile, col
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
df.show()

# defining 4 buckets
num_tiles = 4
# window specification to give running no to each record
window_spec = Window.orderBy("order_col")
#window_spec = Window.orderBy(col(lit(100))
# using ntile window function to divide the data into 4 buckets
df = df.withColumn("bucket", ntile(num_tiles).over(window_spec))
df.show(truncate=False)
# selecting only records from 3rd bucket
mod_df = df.filter(col("bucket") == 3).select("name", "age")
mod_df.show()
