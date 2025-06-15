from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower

spark = SparkSession.builder.appName("Query1_AggravatedAssaultAgeGroups").getOrCreate()
df1 = spark.read.csv("hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv", header=True, inferSchema=True)
df2 = spark.read.csv("hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv", header=True, inferSchema=True)

def trim_col_names(df):
    trimmed_cols = [col_name.strip() for col_name in df.columns]
    return df.toDF(*trimmed_cols)

df1 = trim_col_names(df1)
df2 = trim_col_names(df2)

# Union the two dfs
df = df1.unionByName(df2)

# Filter for "aggravated assault" in the description
df_filtered = df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))

# Create age groups using the correct column name
df_age_groups = df_filtered.withColumn(
    "age_group",
    when(col("Vict Age") < 18, "Children")
    .when((col("Vict Age") >= 18) & (col("Vict Age") <= 24), "Young adults")
    .when((col("Vict Age") >= 25) & (col("Vict Age") <= 64), "Adults")
    .when(col("Vict Age") > 64, "Elderly")
    .otherwise("Unknown")
)


# Group by age_group, count, and sort descending
result = (
    df_age_groups.groupBy("age_group")
    .count()
    .orderBy(col("count").desc())
)

# Save results
result.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query1")
