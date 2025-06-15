from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Query1_DataFrame_UDF").getOrCreate()

df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2010_2019")
df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2020_2025")

df = df1.unionByName(df2)

df_filtered = df.filter(lower(col("Crm Cd Desc")).contains("aggravated assault"))

def classify_age(age):
    if age is None:
        return "Unknown"
    elif age < 18:
        return "Children"
    elif 18 <= age <= 24:
        return "Young adults"
    elif 25 <= age <= 64:
        return "Adults"
    elif age > 64:
        return "Elderly"
    else:
        return "Unknown"

age_group_udf = udf(classify_age, StringType())

df_grouped = df_filtered.withColumn("age_group", age_group_udf(col("Vict Age")))
result = df_grouped.groupBy("age_group").count().orderBy(col("count").desc())
result.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query1_df_udf", header=True)
