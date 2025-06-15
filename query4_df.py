from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.getOrCreate()

# crime files
c10_19 = spark.read.csv(
    "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv",
    header=True, inferSchema=True)
c20_25 = spark.read.csv(
    "hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv",
    header=True, inferSchema=True)

# police stations (X,Y already in lon/lat degrees)
stations = (
    spark.read.csv(
        "hdfs://hdfs-namenode:9000/user/root/data/LA_Police_Stations.csv",
        header=True, inferSchema=True)
    .select(
        F.col("DIVISION").alias("division"),
        F.col("X").alias("station_lon"),
        F.col("Y").alias("station_lat")
    )
)

# mo-code dictionary
mo_raw = spark.read.text(
    "hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt")
mods = (
    mo_raw.select(
        F.split("value", r"\s+", 2).getItem(0).cast("int").alias("mo_code"),
        F.lower(F.expr("substring(value, instr(value,' ')+1)")).alias("mo_desc")
    )
)

# incidents with weapon/gun
crime = (
    c10_19.withColumnRenamed("AREA ", "AREA").unionByName(c20_25)
    .join(F.broadcast(mods), F.col("Mocodes") == F.col("mo_code"), "inner")
    .filter(F.col("mo_desc").contains("weapon") | F.col("mo_desc").contains("gun"))
    .filter(
        (F.col("LAT").isNotNull()) & (F.col("LON").isNotNull()) &
        (F.col("LAT") != 0) & (F.col("LON") != 0)
    )
    .drop("mo_code", "mo_desc")
)

# cross join 
crossed = crime.crossJoin(stations)

# haversine distance (km)
R = 6371.0
crossed = (
    crossed.withColumn("lat1", F.radians("LAT"))
           .withColumn("lat2", F.radians("station_lat"))
           .withColumn("dlat", F.radians(F.col("station_lat") - F.col("LAT")))
           .withColumn("dlon", F.radians(F.col("station_lon") - F.col("LON")))
           .withColumn(
               "a",
               F.pow(F.sin(F.col("dlat") / 2), 2) +
               F.cos("lat1") * F.cos("lat2") *
               F.pow(F.sin(F.col("dlon") / 2), 2)
           )
           .withColumn(
               "distance",
               2 * F.lit(R) * F.atan2(F.sqrt("a"), F.sqrt(1 - F.col("a")))
           )
)

# nearest station per incident
w = Window.partitionBy("DR_NO").orderBy("distance")
nearest = crossed.withColumn("rn", F.row_number().over(w)).filter("rn = 1")

# aggregate
result = (
    nearest.groupBy("division")
           .agg(
               F.count("*").alias("incident_count"),
               F.round(F.avg("distance"), 3).alias("average_distance")
           )
           .orderBy(F.col("incident_count").desc())
)

# save
result.write.mode("overwrite").csv(
    "hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/results/query4",
    header=True
)

spark.stop()
