from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("emp_gender", StringType(), True),
    StructField("emp_age", IntegerType(), True),
    StructField("emp_salary", IntegerType(), True),
    StructField("emp_manager", StringType(), True)
])

# creating the dataframe !
data = [
    (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
    (2, "Aarav Sharma", "Male", 28, 55000, "Zara Singh"),
    (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel"),
    (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
    (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
    (6, "Naina Verma", "Female", 31, 72000, "Arjun Patel"),
    (1, "Arjun Patel", "Male", 30, 60000, "Aarav Sharma"),
    (4, "Priya Reddy", "Female", 32, 65000, "Aarav Sharma"),
    (5, "Aditya Kapoor", "Male", 28, 58000, "Zara Singh"),
    (10, "Anaya Joshi", "Female", 27, 59000, "Aarav Sharma"),
    (11, "Rohan Malhotra", "Male", 36, 73000, "Zara Singh"),
    (3, "Zara Singh", "Female", 35, 70000, "Arjun Patel")
]

spark = SparkSession.builder.appName("DE with Dhairy channel").getOrCreate()

df = spark.createDataFrame(data=data, schema=schema)

df.show()

# Method - 1 using group by method
group_df = df.groupby(df.columns).count().where(col("count") > 1).drop("count")

group_df.show()

# Method -2 Window function

from pyspark.sql import Window

partition_ = Window.partitionBy(df.columns)

df.withColumn("count", count("emp_id").over(partition_)).filter(col("count")>1).drop_duplicates().show()
