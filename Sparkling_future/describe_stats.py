from pyspark.sql import SparkSession
from pyspark.sql.functions import aggregate, sum, col, when, lit,min,max

spark = SparkSession.builder.appName("check list of columns").getOrCreate()

columns = ["product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image"]

order_df = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/order_items.csv"). \
    toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal",
         "order_item_product_price")

order_df.show(50)

order_df.select("order_item_subtotal").describe().show()

order_df.select("order_item_subtotal", "order_item_product_price").describe().show()

order_df.select([max("order_item_subtotal"), min("order_item_product_price")]).show()
