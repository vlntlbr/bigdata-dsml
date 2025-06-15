from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectMOFile").getOrCreate()

# Read file as raw text
mo_raw = spark.read.text("hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt")

# Show first 20 lines to inspect structure
mo_raw.show(20, truncate=False)

mo_raw.printSchema()


# Split based on fixed width (first 4 characters are the code)
mo_df = mo_raw.select(
    mo_raw.value.substr(1, 4).cast("int").alias("Code"),
    mo_raw.value.substr(6, 1000).alias("Description")
)

mo_df.show(10, truncate=False)
mo_df.printSchema()

# Optionally save to Parquet
mo_df.write.mode("overwrite").parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/MO_Codes")
