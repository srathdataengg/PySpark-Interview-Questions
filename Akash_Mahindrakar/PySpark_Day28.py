"""
🌟 PYSPARK Practice/Interview Problem  28
========================================
Your mission, should you choose to accept it, is to showcase your PYSPARK prowess with the following tasks:

1️⃣ Write down the query to fetch Project name assign to more than one Employee
2️⃣ Get employee name, project name order by firstname from "EmployeeDetail" and"ProjectDetail"
for those employee which have assigned project already.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Input data
data = [
    (1, "Vikas", "Ahlawat", 600000.0, "2013-02-15 11:16:28.290", "IT", "Male"),
    (2, "nikita", "Jain", 530000.0, "2014-01-09 17:31:07.793", "HR", "Female"),
    (3, "Ashish", "Kumar", 1000000.0, "2014-01-09 10:05:07.793", "IT", "Male"),
    (4, "Nikhil", "Sharma", 480000.0, "2014-01-09 09:00:07.793", "HR", "Male"),
    (5, "anish", "kadian", 500000.0, "2014-01-09 09:31:07.793", "Payroll", "Male")
]
schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Salary", DoubleType(), True),
    StructField("Joining_Date", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Gender", StringType(), True)
])

spark = SparkSession.builder.appName("Question no 28").getOrCreate()

df = spark.createDataFrame(data, schema)

df.show()

pro_schema = StructType([
    StructField("Project_DetailID", IntegerType(), True),
    StructField("Employee_DetailID", IntegerType(), True),
    StructField("Project_Name", StringType(), True)
])

# Create the data as a list of tuples
pro_data = [
    (1, 1, "Task Track"),
    (2, 1, "CLP"),
    (3, 1, "Survey Management"),
    (4, 2, "HR Management"),
    (5, 3, "Task Track"),
    (6, 3, "GRS"),
    (7, 3, "DDS"),
    (8, 4, "HR Management"),
    (9, 6, "GL Management")
]

pro_df = spark.createDataFrame(data=pro_data, schema=pro_schema)

pro_df.show()

# Write down the query to fetch project name assign to more than one employee

# df.join(pro_df, df['employeeid'] == pro_df['employee_detailid']).show()

pro_df.groupby(col("project_name")) \
    .agg(count("*").alias("count_pro")) \
    .filter(col("count_pro") > 1).show()

# Get employee name, project name order by firstname from "EmployeeDetail" and"ProjectDetail"
# for those employee which have assigned project already.

emp_df = df.join(pro_df, df['employeeid'] == pro_df['employee_detailid'], "inner") \
    .orderBy(col("first_name").asc()) \
    .select(concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"), "project_name")


emp_df.show()
