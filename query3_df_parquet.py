from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

spark = SparkSession.builder.appName("Query3_DataFrame_parquet").getOrCreate()


# --- Load from Parquet
pop_parq = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/2010_Census_Populations_by_Zip_Code")
inc_parq = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_income_2015")
print(pop_parq.columns)
print(inc_parq.columns)

# Preprocessing: clean $ from income column
def preprocess(income_df):
    return income_df.withColumn(
        "Estimated Median Income",
        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
    )

# --- Join and compute per capita income
def compute_per_capita(pop, inc):
    pop = pop.withColumn("Zip Code", col("Zip Code").cast("string"))
    inc = inc.withColumn("Zip Code", col("Zip Code").cast("string"))
    joined = pop.join(inc, on="Zip Code", how="inner")
    joined.explain(True)
    joined.show(5)
    result = joined.withColumn(
        "avg_income_per_person",
        (col("Estimated Median Income") / col("Average Household Size")).cast("double")
    )
    return result.select("Zip Code", "avg_income_per_person")


# From Parquet
parq_result = compute_per_capita(pop_parq, preprocess(inc_parq))
parq_result.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query3_df_parquet", header=True)
