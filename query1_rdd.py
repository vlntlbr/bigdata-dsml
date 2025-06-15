from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query1_RDD").getOrCreate()

df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2010_2019")
df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2020_2025")

df = df1.unionByName(df2)

def is_aggravated(desc):
    return desc and "aggravated assault" in desc.lower()

def group_age(age):
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

rdd = df.select("Crm Cd Desc", "Vict Age").rdd

result_rdd = (
    rdd.filter(lambda row: is_aggravated(row[0]))
       .map(lambda row: (group_age(row[1]), 1))
       .reduceByKey(lambda x, y: x + y)
       .sortBy(lambda x: -x[1])
)

result_df = result_rdd.toDF(["age_group", "count"])
result_df.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query1_rdd", header=True)
