# Big-Data DSML Project 2025, Chrysovalanti Lymperi (03400257)

## Overview  
This repository contains the full Spark workflows developed for the “Big-Data” course project (NTUA Data Science and Machine Learning MSc., Spring emester 2025). 
Four analytical queries are implemented, each in multiple styles (DataFrame, SQL, RDD), together with scaling experiments that measure execution time under different executor configurations.

## Repository structure  
`query1_rdd.py`, `query1_df*.py` Age-group distribution of “aggravated assault” victims  
`query2_df*.py`, `query2_sql.py`, `query2_rdd.py` Top-3 divisions per year by closed-case rate  
`query3_df*.py`, `query3_rdd.py` Median income per capita by ZIP code  
`query4_df.py` Weapon-related incidents and average distance to the nearest police division  
`report.pdf`  report delivered for assessment

## Datasets (HDFS)  
Crime reports 2010-2019 `hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv`  
Crime reports 2020-2025 `hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv`  
Police stations `hdfs://hdfs-namenode:9000/user/root/data/LA_Police_Stations.csv`  
MO Codes dictionary `hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt`  
2010 Census by ZIP `hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv`  
Median income 2015 `hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv`

## How to run a query  
Place each `*.py` file on HDFS.  
Submit with Spark on Kubernetes, e.g. for Query 4 under the 2 × 4 c / 8 GB layout  
