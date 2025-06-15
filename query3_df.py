from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

spark = SparkSession.builder.appName("LA_Avg_Income_Per_Person").getOrCreate()

# Read census population data
population = spark.read.csv(
    "hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv",
    header=True, inferSchema=True
)

# Read income data
income = spark.read.csv(
    "hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv",
    header=True, inferSchema=True
)

# Standardize 'Zip Code' to string type
population = population.withColumn("Zip Code", col("Zip Code").cast("string"))
income = income.withColumn("Zip Code", col("Zip Code").cast("string"))

# Clean up the "Estimated Median Income" column: remove $ and commas, cast to double
income = income.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
)

# Join datasets on Zip Code
joined = population.join(income, on="Zip Code", how="inner")

# Check if "Average Household Size" exists
if "Average Household Size" in joined.columns:
    # Calculate average income per person
    result = joined.withColumn(
        "avg_income_per_person",
        (col("Estimated Median Income") / col("Average Household Size")).cast("double")
    )
else:
    # Alternative calculation if only Total Population and Total Households are available
    result = joined.withColumn(
        "avg_income_per_person",
        (col("Estimated Median Income") * col("Total Households") / col("Total Population")).cast("double")
    )

# Select the relevant columns
output = result.select("Zip Code", "avg_income_per_person")

# write to HDFS
output.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query3")

# Show sample results
output.show(20)
