from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, percent_rank, ntile, cume_dist, col
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

simpleData = (("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]

spark = SparkSession.builder.appName("Window function example").getOrCreate()

df = spark.createDataFrame(data=simpleData, schema=columns)

df.show()

# row_number

analytic_df = df.withColumn("row_number", row_number().over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("rank", rank().over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("dense_rank", dense_rank().over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("percent_rank", percent_rank().over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("ntile", ntile(2).over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("cume_dist", cume_dist().over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("lag", lag("salary", 2).over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("lead", lead("salary", 2).over(Window.partitionBy("department").orderBy("salary")))

analytic_df.show()

"""
PySpark’s window aggregate functions, such as sum(), avg(), and min(), compute aggregated values within specified 
window partitions. These functions perform calculations across rows related to each row in the partition, 
enabling cumulative or comparative analyses within specific groupings. 
"""

from pyspark.sql.functions import sum, avg, min, max, row_number

windowAggSpec = Window.partitionBy("department")

agg_df = df.withColumn("row_number", row_number().over(Window.partitionBy("department").orderBy("salary"))) \
    .withColumn("sum", sum(col("salary")).over(windowAggSpec)) \
    .withColumn("min", min("salary").over(windowAggSpec)) \
    .withColumn("max", max("salary").over(windowAggSpec)) \
    .withColumn("avg", avg("salary").over(windowAggSpec)) \
    .where(col("row_number") == 1).select("department", "sum", "min", "max", "avg")

agg_df.show()
