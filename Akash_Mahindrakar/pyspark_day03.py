"""
Write a Pyspark program to report the first name, last name, city, and state of each person in the
Person dataframe. If the address of a personId is not present in the Address dataframe,
report null instead.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema for the 'persons' table
persons_schema = StructType([
    StructField("personId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True)
])
# Define schema for the 'addresses' table
addresses_schema = StructType([
    StructField("addressId", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])
# Define data for the 'persons' table
persons_data = [
    (1, 'Wang', 'Allen'),
    (2, 'Alice', 'Bob')
]
# Define data for the 'addresses' table
addresses_data = [
    (1, 2, 'New York City', 'New York'),
    (2, 3, 'Leetcode', 'California')
]

spark = SparkSession.builder.appName("3rd program").getOrCreate()

persons_df = spark.createDataFrame(data=persons_data, schema=persons_schema)
persons_df.show()

addresses_df = spark.createDataFrame(data=addresses_data, schema=addresses_schema)
addresses_df.show()

result_df = persons_df.join(addresses_df, persons_df["personId"] == addresses_df["personId"], "left") \
    .select(persons_df["personId"], persons_df["firstName"], persons_df["lastName"], addresses_df["state"])

result_df.show()
