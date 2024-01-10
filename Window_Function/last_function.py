from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("First and Last function").getOrCreate()

data = [("C1", "2023-06-01", 100.0),
        ("C1", "2023-06-02", 150.0),
        ("C1", "2023-06-03", 200.0),
        ("C2", "2023-06-01", 50.0),
        ("C2", "2023-06-02", 75.0),
        ("C2", "2023-06-03", 100.0)
        ]
schema = ["customer_id", "transaction_date", "salary"]

df = spark.createDataFrame(data, schema)

first_df = df.withColumn("first_value", first("transaction_date").over(Window.partitionBy("customer_id")))

first_df.show()

windowSpec = Window.partitionBy("transaction_date")

last_df = df.withColumn("last_value", last("transaction_date").over(windowSpec))

last_df.show()

final_df = df.withColumn("first_transaction_value",
                         first("transaction_date").over(Window.partitionBy("customer_id"))) \
    .withColumn("last_transaction_value",
                last("transaction_date").over(Window.partitionBy("customer_id"))) \
    .drop("transaction_date", "salary")

final_df.show()
