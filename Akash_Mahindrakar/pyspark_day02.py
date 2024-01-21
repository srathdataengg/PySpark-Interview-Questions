"""
Write an pyspark to find the ctr of each, round the ctr to 2 decimal
order the ctr in descending order and ad_id in ascending order
ctr = clicked/(clicked +viewed)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import when, count, col, sum, round

# Define the schema for the Ads table
schema = StructType([
    StructField('AD_ID', IntegerType(), True)
    , StructField('USER_ID', IntegerType(), True)
    , StructField('ACTION', StringType(), True)
])
# Define the data for the Ads table
data = [
    (1, 1, 'Clicked'),
    (2, 2, 'Clicked'),
    (3, 3, 'Viewed'),
    (5, 5, 'Ignored'),
    (1, 7, 'Ignored'),
    (2, 7, 'Viewed'),
    (3, 5, 'Clicked'),
    (1, 4, 'Viewed'),
    (2, 11, 'Viewed'),
    (1, 2, 'Clicked')]

spark = SparkSession.builder.appName("question-2").getOrCreate()
ads_df = spark.createDataFrame(data, schema)
ads_df.show()

result_df = ads_df.groupby("ad_id"). \
    agg(
    sum(when(ads_df["action"] == "Clicked", 1).otherwise(0)).alias("click_count"),
    sum(when(ads_df["action"] == "Viewed", 1).otherwise(0)).alias("viewed_count")
).withColumn("ctr", round(col("click_count") / (col("click_count") + col("viewed_count")), 2))\
    .orderBy("ctr")

result_df.show()
