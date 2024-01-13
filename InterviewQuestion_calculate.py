"""
InterView Question 2
Imagine you have a PySpark DataFrame named df_employee with the following schema:

root
 |-- employee_id: integer
 |-- employee_name: string
 |-- salary: double
 |-- department: string
 |-- hire_date: date
Your task is to perform the following operations:

1)Identify and count the number of null values in each column.
2)Replace null values in the salary column with the mean salary of all employees.
3)Replace null values in the department column with a default value of "Unknown."
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, when, col
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType

data = [
    (1, "John Doe", 5000.0, "HR", "2022-01-15"),
    (2, "Alice Smith", None, "IT", "2022-02-20"),
    (3, "Bob Simpson", 6000.0, None, "2022-03-20"),
    (4, "Eva Williams", 5500.0, "Finance", None)
]

# schema = ["employee_id", "employee_name", "salary", "department", "Hiredate"]
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("department", StringType(), True),
    StructField("hiredate", StringType(), True)
])

spark = SparkSession.builder.appName("employee_calculation").getOrCreate()

df = spark.createDataFrame(data, schema)

df.show()
df.printSchema()

# count the null counts

null_counts = df.agg(*(count(when(col(c).isNull(), c)).alias(c) for c in df.columns))
null_counts.show()

# change the salary to mean salary where salary is Null
from pyspark.sql.functions import mean, count

# mean salary

mean_salary = df.select(mean(col("salary"))).collect()[0][0]
# mean_salary.show()
print(mean_salary)

# employee_df = df.withColumn("salary", when(col("salary").isNull(), mean_salary).otherwise(col("salary")))

# another way to do that

employee_df = df.na.fill(mean_salary, subset=['salary'])

employee_df.show()

employee_df = df.na.fill("unknown", subset=['department']) \
    .na.fill("9999-99-99", subset=['hiredate'])

employee_df.show(truncate=False)
