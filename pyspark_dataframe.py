from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum, col, desc, asc, when
from pyspark.sql import SparkSession

data = [(1, "John", 30, "Sales", 50000),
        (2, "Alice", 28, "Marketing", 60000),
        (3, "Bob", 32, "Finance", 55000),
        (4, "Sarah", 29, "Sales", 52000),
        (5, "Mike", 31, "Finance", 58000)
        ]
schema = StructType([StructField("id", IntegerType(), True),
                     StructField("name", StringType(), True),
                     StructField("age", IntegerType(), True),
                     StructField("department", StringType(), True),
                     StructField("salary", IntegerType(), True)
                     ])

spark = SparkSession.builder.appName("Spark dataframe").getOrCreate()
emp_df = spark.createDataFrame(data=data, schema=schema)
emp_df.show()

top3dept = emp_df.groupby("department").agg(sum('salary').alias("total_salary"))

top3dept = top3dept.orderBy(desc("total_salary")).limit(1).select("department")

top3dept.show()

sales_30_df = emp_df.filter((emp_df.age >= 30) & (emp_df.department == "Sales"))

sales_30_df1 = emp_df.filter((col("age") >= 30) & (col("department") == "Sales"))

sales_30_df.show()
sales_30_df1.show()

diff_df = emp_df.withColumn("avg_salary", avg("salary").over(Window.partitionBy("department"))) \
    .withColumn("diff_salary", col("avg_salary") - col("salary"))

diff_df.show()

# calculate the sum of salaries for employees whose name starts with 'J'

j_df = emp_df.filter(col("name").like("J%")).agg(sum("salary").alias("total_salary"))
j_df.show()

sort_df = emp_df.orderBy(asc("age"), desc("salary"))

sort_df.show()

fin_df = emp_df.withColumn("department",
                           when(col("department") == "Finance", "financial_services").otherwise(col("department")))

fin_df.show()

# calculate the percentage of total_salary each employee contributes to each department

contribution_df = emp_df.withColumn("total_salary_department",
                                    sum("salary").over(Window.partitionBy(col("department")))) \
    .withColumn("percentage_salary", (col("salary") / col("total_salary_department")) * 0.1)

contribution_df.show()
