from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, reverse, asc, desc, year, max, min, avg, aggregate, concat, lit, \
    month, count
from pyspark.sql import Window

spark = SparkSession.builder.appName("emp_dept analysis").getOrCreate()

dept = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/departments.csv",
                      header=True)

dept.show(truncate=False)

emp = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/employees.csv", header=True)

emp.show(truncate=False)

# Select employee’s first name, last name, job_id, and
# salary whose first name starts with alphabet ‘S’

df = emp. \
    filter(col("first_name").like("S%")) \
    .select(col("first_name"), col("last_name"), col("job_id"), col("salary"))

df.show()

# find the employees who joined after 15th of the month

joining_date = emp.filter(dayofmonth('hire_date') > 15). \
    select(col("first_name"), col("last_name"), col("job_id"), col("hire_date"))

joining_date.show(truncate=False)

# Display the employee first name and first name in reverse order.

reverse_firstname = emp.select("first_name").withColumn("reverse_name", reverse(col("first_name")))
reverse_firstname.show()

# Display the 5 least earning employee

least_df = emp.select('first_name', 'last_name', 'salary').orderBy(asc('salary')).limit(50)

least_df.show()

# find the employees who hired in 80s

hire_df = emp.select('first_name', 'last_name', 'hire_date'). \
    filter(year('hire_date').between(1980, 1990))
hire_df.show()

# find the maximum salary from each department
joined_df = emp.join(dept, emp['department_id'] == dept['department_id'], 'left')
joined_df.show()

max_salary = joined_df.select('department_name', 'salary') \
    .groupby('department_name').agg(max('salary').alias("Max_salary"))

max_salary.show()

# Find employees who earn more thn average salary


average_salary = emp.groupby('salary').agg(avg("salary").alias('avg_salary')).first()["avg_salary"]
# avg_salary = emp.agg({'salary': avg})
# avg_salary.show()

avg_salary_final = emp.filter(col("salary") > average_salary).select('first_name', 'last_name', 'salary')
avg_salary_final.show()

# window function approach
window_spec = Window.partitionBy("department_id")
avg_salary1 = emp.withColumn('avg_salary', avg("salary").over(window_spec))

avg_salary1 = avg_salary1.filter(col('salary') > col('avg_salary')).select('first_name', 'last_name', 'salary')
avg_salary1.show()

# find the employees who joined in August 1994

result = emp.filter((year('hire_date') == 1994) & (month('hire_date') == 8)).select(
    concat('first_name', lit(' '), 'last_name').alias("emp_name"), 'hire_date')
result.show()

# find the salary range of employees - max salary, min salary and avg salary

stats_df = emp.select(avg(col("salary").alias("avg_salary")), max(col("salary").alias("max_salary")), \
                      min(col("salary").alias("min_salary")))

stats_df.show()

# get the count of employees hired year wise

emp_year_count_df = emp.groupby(year(col("hire_date")).alias("hire_year")). \
    agg(count(col("employee_id")).alias("employee_count")). \
    orderBy(asc("employee_count"))

emp_year_count_df.show()

# find the count of employees department wise

dept_count_df = joined_df.groupby("department_name").agg(count(col("employee_id")).alias("emp_count")) \
    .orderBy(asc("emp_count"))

dept_count_df.show()

# find count of employees under each manager in descending order.

emp_manager_df = emp.groupby('manager_id').agg(count('employee_id').alias("emp_count"))

emp_manager_df.show()

emp_df = emp.alias("emp_df")
mgr_df = emp.alias("mgr_df")
# emp_df.show()
# mgr_df.show()

# joined_df1 = emp_df.join(mgr_df, emp_df['employee_id'] == mgr_df['manager_id'], "inner")
joined_df1 = mgr_df.join(emp_df, col("emp_df.manager_id") == col("mgr_df.manager_id"), "inner")

joined_df1.show()

emp_manager_df1 = joined_df1.groupby(concat('mgr_df.first_name', lit(' '), 'mgr_df.last_name').alias('manager')).agg(
    count(col("emp_df.employee_id")).alias('reportee_count')).orderBy(asc('reportee_count'))
emp_manager_df1.show()

emp_manager_salary_df = joined_df1.select(concat(col("emp_df.first_name"), lit(" "), col("emp_df.last_name")).alias("employee_name"),
                                          col("emp_df.salary"),
                                          concat(col("mgr_df.first_name"),lit(" "),col("mgr_df.last_name")).alias("manager_name"),
                                          col("mgr_df.salary"))
emp_manager_salary_df.show(30)

emp_high_sal_df = emp.orderBy(desc("salary"))
#emp_high_sal_df1 = emp.rank("salary").over(Window.orderBy("salary"))
emp_high_sal_df.show()
