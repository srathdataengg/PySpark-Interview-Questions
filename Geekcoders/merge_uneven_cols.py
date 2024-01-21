from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("merge_uneven_cols").getOrCreate()

simpleData = [(1, "Sagar", "CSE", "UP", 80),
              (2, "Shivam", "IT", "MP", 86),
              (3, "Muni", "Mech", "AP", 70)]
columns = ["ID", "Student_Name", "Department_Name", "City", "Marks"]

simpleDF = spark.createDataFrame(simpleData, columns)

simpleDF.show()

simpleData2 = [(5, "Raj", "CSE", "HP"),
               (7, "Kunal", "Mech", "Rajasthan")]

columns1 = ["Id", "StudentName", "Department", "City"]

simpleDF2 = spark.createDataFrame(simpleData2, columns1)

simpleDF2.show()

mergeDF = simpleDF.select("ID", "Student_Name", "Department_Name", "City").union(simpleDF2)

mergeDF.show()
