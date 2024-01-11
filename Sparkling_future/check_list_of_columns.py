"""
Check if the columns of a dataframe consists of required columns according to BRD.
Real- Time Data Quality scenario
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check list of columns").getOrCreate()

df = spark.read.csv("/Users/soumyakantarath/Desktop/PySpark-Interview-Questions/datasets/Orders.csv", header=True)

df.show(5)

list1 = ['SALES', 'Profit']

df_cols_list = df.columns
lowerlist1 = (i.lower() for i in list1)

# Python all function checks if all elements are True then true otherwise false
# python list operations allow to write multiple expression in a list Ex:- res =[i.upper() for i in test_list]

if all([i in df_cols_list for i in lowerlist1]):
    print("True")
else:
    print("false")
