from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

spark = SparkSession.builder.appName("Query3_DataFrame").getOrCreate()

# --- Load from CSV
pop_csv = spark.read.csv("hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv", header=True, inferSchema=True)
inc_csv = spark.read.csv("hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv", header=True, inferSchema=True)

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

    result = joined.withColumn(
        "avg_income_per_person",
        (col("Estimated Median Income") / col("Average Household Size")).cast("double")
    )
    return result.select("Zip Code", "avg_income_per_person")

# From CSV
csv_result = compute_per_capita(pop_csv, preprocess(inc_csv))
csv_result.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query3_df_csv", header=True)

