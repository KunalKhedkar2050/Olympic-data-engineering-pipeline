# data_processing.py - Clean & Process Olympics data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, countDistinct
import config

# Start Spark Session
spark = SparkSession.builder.appName("OlympicsDataProcessing").getOrCreate()

# Read raw data
df = spark.read.csv(config.RAW_DATA_PATH, header=True, inferSchema=True)

# Data Cleaning & Transformation
df_clean = df.dropna()
df_clean = df_clean.withColumn("Year", col("Year").cast("int"))
df_clean = df_clean.withColumn("Age", col("Age").cast("int"))
df_clean = df_clean.withColumn("Height", col("Height").cast("float"))
df_clean = df_clean.withColumn("Weight", col("Weight").cast("float"))
df_clean = df_clean.withColumn("Medal_Count", when(col("Medal").isNotNull(), 1).otherwise(0))

# Save processed data as Delta Table
df_clean.write.format("delta").mode("overwrite").save(config.PROCESSED_DATA_PATH)

print("Data Processing Completed")
