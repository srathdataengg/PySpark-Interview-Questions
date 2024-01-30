"""
write a pyspark to remove the duplicate emails.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

emails_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True)
])
# Define data for the "emails" table
emails_data = [
    (1, 'a@b.com'),
    (2, 'c@d.com'),
    (3, 'a@b.com')
]

spark = SparkSession.builder.appName("day06").getOrCreate()
emails_data_df = spark.createDataFrame(data=emails_data, schema=emails_schema)
emails_data_df.show()

emails_data_df.dropDuplicates(['email']).show()
