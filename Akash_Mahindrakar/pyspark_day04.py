"""
04_Employees Earning More Than Their Managers
Write a Pyspark program to find Employees Earning More Than Their
Managers
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Define the schema for the "employees"
employees_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("managerId", IntegerType(), True)
])
# Define data for the "employees"
employees_data = [
    (1, 'Joe', 70000, 3),
    (2, 'Henry', 80000, 4),
    (3, 'Sam', 60000, None),
    (4, 'Max', 90000, None)
]

spark = SparkSession.builder.appName("4th question").getOrCreate()
emp_df = spark.createDataFrame(data=employees_data, schema=employees_schema)

emp_df.show()
