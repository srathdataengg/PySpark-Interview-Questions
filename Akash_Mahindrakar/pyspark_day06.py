from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
# Define data for the "Customers"
customers_data = [
    (1, 'Joe'),
    (2, 'Henry'),
    (3, 'Sam'),
    (4, 'Max')
]
# Define the schema for the "Orders"
orders_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customerId", IntegerType(), True)
])

orders_data = [
    (1, 3),
    (2, 1)
]

spark = SparkSession.builder.appName("day06").getOrCreate()
customers_data_df = spark.createDataFrame(data=customers_data, schema=customers_schema)
customers_data_df.show()
orders_data_df = spark.createDataFrame(data=orders_data, schema=orders_schema)
orders_data_df.show()

result = customers_data_df.join(orders_data_df, customers_data_df['id'] == orders_data_df['customerId'], "left_anti")
result.show()
