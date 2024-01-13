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
    (4, "Eva Williams", 5500.0, "Finance", None),
    (11, "John Doe", 50000.0, "HR", "2022-01-15"),
    (21, "Alice Derby", 75000.0, "IT", "2022-02-20"),
    (31, "Bob charles", 60000.0, "Marketing", "2022-03-20"),
    (41, "Eva Johnson", 55000.0, "Sales", None)
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
from pyspark.sql.functions import mean, count, when, col

# mean salary

mean_salary = df.select(mean(col("salary"))).collect()[0][0]
# mean_salary.show()
print(mean_salary)

# employee_df = df.withColumn("salary", when(col("salary").isNull(), mean_salary).otherwise(col("salary")))

# another way to do that

employee_df = df.na.fill(mean_salary, subset=['salary'])

employee_df.show()

employee_df = employee_df.na.fill("unknown", subset=['department']) \
    .na.fill("9999-99-99", subset=['hiredate'])

employee_df.show(truncate=False)

"""
Calculate Bonus:

Add a new column named bonus based on the following conditions:
If the employee is in the "Sales" department and has a salary greater than 5000, the bonus is 10% of the salary.
If the employee is in the "Marketing" department and has a salary greater than 60000, the bonus is 15% of the salary.
For all other cases, the bonus is 5% of the salary.
Filter Data:

Create a new DataFrame named high_salary_employees containing only the rows where the salary is greater than 70000.
Sort and Select:

Sort the high_salary_employees DataFrame in descending order based on the salary column.
Select the top 5 records from the sorted DataFrame.
"""

employee_df = employee_df.withColumn("bonus",
                                     when((col("salary") > 50000) & (col("department") == "Sales"),
                                          col("salary") * 0.10)
                                     .when((col("salary") > 60000) & (col("department") == "Marketing"),
                                           col("salary") * 0.15) \
                                     .otherwise(col("salary") * 0.5))

employee_df.show()

employee_df = employee_df.sort("salary")

employee_df.show()

# Create a new dataframe named high_salary_employees containing only the rows where the salary is greater than 70000

high_salary_df = employee_df.filter(col("salary")>60000)
high_salary_df.show()

# Sort the high salary employees dataframe in descending order based on the salary column.
# Sort the top 5 records from the sorted dataframe.


salary_df = employee_df.sort(col("salary").desc())
salary_df.show()
