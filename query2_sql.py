from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year

spark = SparkSession.builder.appName("Query2_SQL").getOrCreate()

# Load parquet data
df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2010_2019")
df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2020_2025")
df = df1.unionByName(df2)

# Create timestamp and year column
df = df.withColumn("DATE_OCC_ts", to_timestamp(df["DATE OCC"], "MM/dd/yyyy hh:mm:ss a"))
df = df.withColumn("year", year("DATE_OCC_ts"))

# Register as SQL temp view
df.createOrReplaceTempView("crimes")

# SQL query using CTEs and ROW_NUMBER()
query = """
WITH base AS (
  SELECT year,
         `AREA NAME` AS precinct,
         COUNT(*) AS total_cases,
         SUM(CASE WHEN `Status Desc` NOT IN ('UNK', 'Invest Cont') THEN 1 ELSE 0 END) AS closed_cases
  FROM crimes
  GROUP BY year, `AREA NAME`
),
rates AS (
  SELECT year,
         precinct,
         ROUND(100.0 * closed_cases / total_cases, 8) AS closed_case_rate,
         ROW_NUMBER() OVER (PARTITION BY year ORDER BY 100.0 * closed_cases / total_cases DESC) AS rank
  FROM base
)
SELECT year, precinct, closed_case_rate, rank AS `#`
FROM rates
WHERE rank <= 3
ORDER BY year, `#`
"""

# Run and write results
result = spark.sql(query)
result.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query2_sql", header=True)
