"""
Your task is to find the policy type with the highest average payment amount per customer.

Write a PySpark code that performs the following steps:

a. Join the Policies and Payments DataFrames on the policy_id column.

b. Group the resulting DataFrame by policy_type and customer_id.

c. Calculate the average payment amount per customer for each policy type.

d. Find the policy type with the highest average payment amount per customer.

e. Display the final result with columns policy_type, customer_id, max_average_payment, and average_payment_amount.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("policy_calculation").getOrCreate()

payment_df = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/payments.csv",
                            header=True)

policy_df = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/policies.csv",
                           header=True)

payment_df.show()
policy_df.show()

result1_df = policy_df.join(payment_df, policy_df.policy_id == payment_df.policy_id, "inner")
# .select("payment_df.policy_id", "policy_df.customer_id", "policy_df.policy_type", "coverage_amount", "premium_amount")

result1_df.show()

group_df = result1_df.groupby("policy_type", "customer_id").agg(avg(col("payment_amount")).alias("avg_amount"))

group_df.show()

avg_payment_customer_df = group_df.groupby("policy_type").agg(max(col("avg_amount")).alias("max_avg_payment"))

avg_payment_customer_df.show()

final_df = avg_payment_customer_df.join(group_df,
                                        avg_payment_customer_df.policy_type == group_df.policy_type,"inner")\
    .drop(group_df.policy_type)

# final_df = final_df.select('policy_type', 'max_avg_payment', 'customer_id', 'avg_amount')

final_df.show()
