from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("Query2_RDD").getOrCreate()

# Read from Parquet
df1 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2010_2019")
df2 = spark.read.parquet("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/LA_Crime_Data_2020_2025")
df = df1.unionByName(df2)

# Select relevant fields
rdd = df.select("DATE OCC", "AREA NAME", "Status Desc").rdd

# Parse and classify records
def parse(row):
    try:
        date = datetime.strptime(row[0], "%m/%d/%Y %I:%M:%S %p")
        year = date.year
        precinct = row[1]
        is_closed = 0 if row[2] in ("UNK", "Invest Cont") else 1
        return ((year, precinct), (1, is_closed))  # (key), (total, closed)
    except:
        return None

parsed = rdd.map(parse).filter(lambda x: x is not None)

# Aggregate: (total cases, closed cases)
aggregated = parsed.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Map to (year, (precinct, closed_case_rate))
with_rates = aggregated.map(lambda x: (
    x[0][0],  # year
    (x[0][1], round(100.0 * x[1][1] / x[1][0], 8))  # (precinct, rate)
))

# Group by year, sort by rate desc, keep top 3
top3 = with_rates.groupByKey().mapValues(lambda records: sorted(list(records), key=lambda r: -r[1])[:3])

# Flatten with ranking
flattened = top3.flatMap(lambda x: [
    (x[0], precinct, rate, rank + 1) for rank, (precinct, rate) in enumerate(x[1])
])

sorted_result = flattened.sortBy(lambda x: (x[0], x[3]))  # sort by year, then rank

result_df = sorted_result.toDF(["year", "precinct", "closed_case_rate", "#"])
result_df.write.mode("overwrite").csv("hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query2_rdd", header=True)
