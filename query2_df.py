from pyspark.sql.functions import col, year, to_timestamp, count, sum, round, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Query2_DataFrame").getOrCreate()

df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2010_2019")
df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2020_2025")
df = df1.unionByName(df2)

df = df.withColumn("DATE_OCC_ts", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
df = df.withColumn("year", year(col("DATE_OCC_ts")))

df = df.withColumn("is_closed", (~col("Status Desc").isin("UNK", "Invest Cont")).cast("int"))

agg = df.groupBy("year", "AREA NAME").agg(
    count("*").alias("total_cases"),
    sum("is_closed").alias("closed_cases")
).withColumn(
    "closed_case_rate",
    round(col("closed_cases") / col("total_cases") * 100, 8)
)

window = Window.partitionBy("year").orderBy(col("closed_case_rate").desc())
ranked = agg.withColumn("rank", row_number().over(window)).filter(col("rank") <= 3)

result = ranked.select(
    "year",
    col("AREA NAME").alias("precinct"),
    "closed_case_rate",
    col("rank").alias("#")
).orderBy("year", "#")

result.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query2_df", header=True)
