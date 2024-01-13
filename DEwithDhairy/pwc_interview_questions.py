"""
Task : Retreive information about consecutive login streaks of employee
    who have logged in for 2 consecutive days.
    For each employee id provide the    emp_id and the number of consecutive days it logegd in.
    the start_date and end_state of the streak.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, lit, to_date, row_number, dayofmonth, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

_data = [(101, '02-01-2024', 'N'),
         (101, '03-01-2024', 'Y'),
         (101, '04-01-2024', 'N'),
         (101, '07-01-2024', 'Y'),
         (102, '01-01-2024', 'N'),
         (102, '02-01-2024', 'Y'),
         (102, '03-01-2024', 'Y'),
         (102, '04-01-2024', 'N'),
         (102, '05-01-2024', 'Y'),
         (102, '06-01-2024', 'Y'),
         (102, '07-01-2024', 'Y'),
         (103, '01-01-2024', 'N'),
         (103, '04-01-2024', 'N'),
         (103, '05-01-2024', 'Y'),
         (103, '06-01-2024', 'Y'),
         (103, '07-01-2024', 'N')
         ]
_schema = ["emp_id", "log_date", "flag"]

spark = SparkSession.builder.appName("PWC Interview Questions").getOrCreate()

# creating the dataframe
df = spark.createDataFrame(data=_data, schema=_schema)
df.show()

# filter out all the N flags as we don't need those rows
df1 = df.select(df.columns).filter(col("flag") == "Y")

# change the log date to time-stamp format from string
df1 = df1.withColumn("log_date", to_date(col("log_date"), "dd-mm-yyyy"))

df1.show()

# add rownumber to the df

row_df = df1.withColumn("rn", row_number().over(Window.partitionBy(col("emp_id")).orderBy(col("log_date"))))

row_df.show()

# Creating the day column


df_rn_day = row_df.withColumn("day_", dayofmonth(col("log_date")))

df_rn_day.show()

# creating the group column
df_group = df_rn_day.withColumn("group_", col("day_") - col("rn"))
df_group.show()

# grouping the emp_id and the group_ columns and finding the min ---> start_date and max --> end_date ,count
df_answer = df_group.groupby(col("emp_id"), col("group_")).agg(
    min(col("log_date")).alias("start_date"),
    max(col("log_date")).alias("end_date"),
    count(lit("1")).alias("consecutive_day")
)

df_answer.show()

# final output
df_final = df_answer.filter(col("consecutive_day") > 1)

df_final.show()

print(3+2)
