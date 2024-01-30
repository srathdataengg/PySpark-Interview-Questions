"""Task - The dataset includes company_id, company_name, and multiple date columns representing the stock prices of
the company. The task is to generate a new dataset with only three columns, "company_name", "Date", "Stock_price". """

from pyspark.sql import SparkSession

data = [
    (1, "Quantum Innovations", 12.5, 14.5, 16.5, 18.5, 20.5),
    (2, "Stellar Solutions", 14.5, 16.5, 18.5, 20.5, 22.5),
    (3, "Nebula Dynamics", 16.5, 18.5, 20.5, 22.5, 24.5),
    (4, "Fusion Enterprises", 18.5, 20.5, 22.5, 24.5, 26.5),
    (5, "Celestial Technologies", 20.5, 22.5, 24.5, 26.5, 28.5),
]

spark = SparkSession.builder.appName("Lipsa_cde").getOrCreate()

df = spark.createDataFrame(data, ["company_id", "company_name", "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04",
                                  "2024-01-05"])

df.show()

# Unpivot
id_cols = ["company_id", "company_name"]
value_columns = [col_name for col_name in df.columns if col_name not in id_cols]
df.unpivot("company_name", value_columns, 'Date', 'Stock_price').show()
