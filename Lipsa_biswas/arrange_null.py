"""
'Arranging a column with null values in ascending or descending order.'

Task - Sort the salary column of the dataframe :
a) in ascending order with NULL values at the top
b) in ascending order with NULL values at the bottom
c) in descending order with NULL values at the top
d) in descending order with NULL values at the bottom
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("myapp").getOrCreate()
data = [
    (1, "Aditya Sen", None),
    (2, "Bikramaditya", 200),
    (3, "Mark T", 100),
    (4, "D K aditya", None),
    (5, "Danny", 500),
    (6, "Eli", 300),
]

df = spark.createDataFrame(data).toDF("Id", "Name", "Salary")
df.show()
df.select("Name", "Salary").orderBy(col("Salary").asc_nulls_first()).show()
df.select("Name", "Salary").orderBy(col("Salary").asc_nulls_last()).show()
df.select("Name", "Salary").orderBy(col("Salary").desc_nulls_first()).show()
df.select("Name", "Salary").orderBy(col("Salary").desc_nulls_last()).show()
