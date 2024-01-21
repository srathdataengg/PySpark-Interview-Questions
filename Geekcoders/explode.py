from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

data = [(1, ["Soumya", "kanta"]),
        (2, ["subhra", "kanta"])
        ]
columns = ["Id", "Name"]

spark = SparkSession.builder.appName("explode").getOrCreate()
df = spark.createDataFrame(data, columns)
df.show()

result_df = df.select(col("Id"), explode(col("Name")))

result_df.show()
