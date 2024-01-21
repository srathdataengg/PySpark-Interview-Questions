from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_extract, substring

spark = SparkSession.builder.appName("extract_name").getOrCreate()

data = [{"email": "john.doe@email.com"},
        {"email": "jane.smith@email.com"},
        {"email": "alice.wonder@email.com"}]

schema = ["email"]

df = spark.createDataFrame(data)

df.show()

df_extract = df.withColumn("first_name", split(col("email"), "\\.")[0]) \
    .withColumn("last_name", split(col("email"), "\\.").getItem(1))

#df_extracted = df.withColumn("last_name", split(col("email"), "\\.").getItem(1))

df_extracted = df_extract.withColumn("last_name", split(col("last_name"), "\\@").getItem(0))

df_extract.show()

df_extracted.show()
