"""
Check the product dataset.
Scenario:
1. Get the total product_price for each product_category_id as total_sales and then add some literal to product_description.
2. Join products with categories and get total sales for each category_id.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check list of columns").getOrCreate()

columns = ["product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image"]

df = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/products.csv"). \
    toDF("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image")

df.show(50)

# to see how many count of products are there for each product_id

df.groupby("product_category_id").count().show()

# Get the total_product_price for each product_category_id as total_sales and then add some literal to product
# descrription

from pyspark.sql.functions import sum, aggregate, lit

df2 = df.groupby("product_category_id").agg(sum("product_price").alias("total_sales")).withColumn("product_description",
                                                                                                  lit(1))
df2.show()

df3 = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/categories.csv"). \
    toDF("category_id", "category_department_id", "category_name")

df3.show(50)

# Join categories table to find the total(sales) based on category_id

df4 = df.join(df3, df["product_category_id"] == df3["category_id"], "inner"). \
    select("category_id", "product_price", "category_name"). \
    groupby("category_id"). \
    agg(sum("product_price").alias("total_sales")).withColumn("product_description", lit("product1"))

df4.show(truncate=False)
