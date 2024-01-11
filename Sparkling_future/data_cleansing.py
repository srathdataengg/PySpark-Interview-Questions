from pyspark.sql import SparkSession
from pyspark.sql.functions import aggregate, sum, col, when, lit, min, max, concat_ws
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("check list of columns").getOrCreate()

columns = ["product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image"]

user_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("multiline", "true") \
    .load("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/user_data.csv")

user_df.show(50)

# user_df = user_df.select("id",
#                          when(col("ind") == col(lit("FN")), col("fname")).otherwise("null").alias("fname"),
#                          when(col("ind") == col(lit("LN")), col("fname")).otherwise("null").alias("lname"),
#                          when(col("ind") == col(lit("AD")),
#                               concat_ws(",", col("fname"), col("lname"), col("apartment"), col("street"), col("city"))). \
#                          otherwise("null").alias("address"),
#                          when(col("ind") == col(lit("PH")), col("fname")).otherwise("null").alias("phone"))
#
# user_df.show()

df2 = user_df.select("id",
                     f.when(f.col("ind") == f.lit("FN"), f.col("fname")).otherwise("null").alias("fname"),
                     f.when(f.col("ind") == f.lit("LN"), f.col("fname")).otherwise("null").alias("lname"),
                     f.when(f.col("ind") == f.lit("AD"),
                            f.concat_ws(",", f.col("fname"), f.col("lname"), f.col("apartment"),
                                        f.col("street"))).otherwise("null").alias("address"),
                     f.when(f.col("ind") == f.lit("PH"), f.col("fname")).otherwise("null").alias("phone"))
df2.show(truncate=False)

df3 = df2.groupby("id").agg(f.min("fname").alias("fname"),
                            f.min("lname").alias("lname"),
                            f.min("address").alias("address"),
                            f.min("phone").alias("phone"))
df3.show(truncate=False)

df4 = df3.filter((f.col("fname") != f.lit("null")) & (f.col("lname") != f.lit("null")))
df4.show()

df5 = df4.withColumn("apartment", f.split(f.col("address"), ',').getItem(0)) \
       .withColumn('street', f.split(f.col("address"), ',').getItem(1)) \
       .withColumn('city', f.split(f.col("address"), ',').getItem(2))\
       .withColumn('country',f.split(f.col("address"), ',').getItem(3))

df5.select("id", "fname", "lname", "apartment", "street", "city", "country", "phone").show(truncate=False)