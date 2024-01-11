"""
Scenario:
get the total order_item_sub_total for each order_item_order_id.
Update the price as 100 for the products which are having single orders, for others have the same price.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import aggregate, sum, col, when, lit

spark = SparkSession.builder.appName("check list of columns").getOrCreate()

columns = ["product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image"]

order_df = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/order_items.csv"). \
    toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal",
         "order_item_product_price")

order_df.show(50)

order_df.filter(col("order_item_order_id") == 2).show()

res_df = order_df.groupby("order_item_order_id").agg(
    sum(when(col("order_item_quantity") > lit("1"), col("order_item_subtotal")). \
        otherwise(lit("100.0"))).alias("total_sales"))
res_df.filter("order_item_order_id ==2").show()
