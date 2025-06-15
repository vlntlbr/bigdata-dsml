from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query3_RDD").getOrCreate()

pop_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/2010_Census_Populations_by_Zip_Code")
inc_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_income_2015")

pop_rdd = pop_df.select("Zip Code", "Average Household Size").rdd.map(lambda row: (str(row[0]), row[1]))
inc_rdd = inc_df.select("Zip Code", "Estimated Median Income").rdd.map(
    lambda row: (str(row[0]), float(str(row[1]).replace("$", "").replace(",", "")))
)

# Join on Zip Code
joined = pop_rdd.join(inc_rdd)  # (Zip Code, (avg_household_size, median_income))

per_capita_income = joined.mapValues(lambda x: round(x[1] / x[0], 2) if x[0] else None) \
                          .filter(lambda x: x[1] is not None)

result_df = per_capita_income.map(lambda x: (x[0], x[1])).toDF(["Zip Code", "avg_income_per_person"])
result_df.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query3_rdd", header=True)
