from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PreprocessToParquet").getOrCreate()

base_input = "hdfs://hdfs-namenode:9000/user/root/data/"
base_output = "hdfs://hdfs-namenode:9000/user/chrysovalantilymperi/data/parquet/"

def trim_col_names(df):
    return df.toDF(*[c.strip() for c in df.columns])

# 1. LA Crime Data 2010–2019
crime1 = spark.read.csv(base_input + "LA_Crime_Data_2010_2019.csv", header=True, inferSchema=True)
crime1 = trim_col_names(crime1)
crime1.write.mode("overwrite").parquet(base_output + "LA_Crime_Data_2010_2019")

# 2. LA Crime Data 2020–
crime2 = spark.read.csv(base_input + "LA_Crime_Data_2020_2025.csv", header=True, inferSchema=True)
crime2 = trim_col_names(crime2)
crime2.write.mode("overwrite").parquet(base_output + "LA_Crime_Data_2020_2025")

# 3. LA Police Stations
stations = spark.read.csv(base_input + "LA_Police_Stations.csv", header=True, inferSchema=True)
stations = trim_col_names(stations)
stations.write.mode("overwrite").parquet(base_output + "LA_Police_Stations")

# 4. Median Household Income by Zip Code
income = spark.read.csv(base_input + "LA_income_2015.csv", header=True, inferSchema=True)
income = trim_col_names(income)
income.write.mode("overwrite").parquet(base_output + "LA_income_2015")

# 5. 2010 Census Populations by Zip Code
census = spark.read.csv(base_input + "2010_Census_Populations_by_Zip_Code.csv", header=True, inferSchema=True)
census = trim_col_names(census)
census.write.mode("overwrite").parquet(base_output + "2010_Census_Populations_by_Zip_Code")

