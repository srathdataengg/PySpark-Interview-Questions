"""
Suppose you are working with a dataset from an insurance company that tracks policy changes over time. The dataset,
stored in a CSV file named "insurance_data.csv," has
The following columns: policy_id, policy_holder, policy_type, premium_amount, and change_date.
1) Your task is to calculate the rolling average of the premium_amount for each policy type over a window of 2 days.
2) Additionally, you need to provide the total premium amount for each policy type.
Implement this using PySpark, specifically utilizing window functions.

Sample data:
policy_id,policy_holder,policy_type,premium_amount,change_date
1,Alice,Auto,500,2023-01-01
2,Bob,Home,700,2023-01-02
3,Alice,Auto,550,2023-01-03
4,Charlie,Auto,600,2023-01-04
5,Bob,Auto,520,2023-01-05
6,Alice,Home,750,2023-01-06
7,Bob,Auto,530,2023-01-07
8,Charlie,Home,720,2023-01-08
9,Alice,Auto,580,2023-01-09
10,Bob,Home,770,2023-01-10
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col
from pyspark.sql.functions import avg, sum,max

spark = SparkSession.builder.appName("Rolling_avergae").getOrCreate()

insurance_df = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/Insurance.csv",
                              header=True)

insurance_df.show()

window_spec = Window.partitionBy("policy_type").orderBy("change_date").rowsBetween(-1, 0)

rolling_avg_df = insurance_df.withColumn("rolling_avg", avg(col("premium_amount")).over(window_spec)
                                         ). \
    withColumn("premium_amount", max(col("premium_amount")).over(window_spec))

rolling_avg_df.show()
