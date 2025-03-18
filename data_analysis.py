# data_analysis.py - Advanced Olympics Data Analysis

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum, countDistinct, first, last, col

import config

# Start Spark Session
spark = SparkSession.builder.appName("OlympicsDataAnalysis").getOrCreate()

# Read processed data from Delta Lake
df = spark.read.format("delta").load(config.PROCESSED_DATA_PATH)

# Top 10 Countries by Medal Count
df_medal_count = df.groupBy("Country").agg(count("Medal").alias("Total_Medals"))
df_medal_count.orderBy("Total_Medals", ascending=False).show(10)

# 🎖 Top 10 Athletes by Medals Won
df_top_athletes = df.groupBy("Athlete").agg(count("Medal").alias("Medals_Won"))
df_top_athletes.orderBy("Medals_Won", ascending=False).show(10)

# Unique Events per Year
df_events_per_year = df.groupBy("Year").agg(countDistinct("Event").alias("Unique_Events"))
df_events_per_year.orderBy("Year").show()

# Country-wise Participation Over Time
df_country_participation = df.groupBy("Year", "Country").agg(countDistinct("Athlete").alias("Total_Athletes"))
df_country_participation.orderBy("Year", "Country").show(10)

# Average Age & BMI per Sport
df_sport_stats = df.groupBy("Sport").agg(
    avg("Age").alias("Avg_Age"), 
    avg(col("Weight") / ((col("Height")/100) * (col("Height")/100))).alias("Avg_BMI")
)
df_sport_stats.orderBy("Avg_Age", ascending=False).show(10)

# Most Successful Countries Over Time
df_successful_countries = df.groupBy("Year", "Country").agg(sum("Medal_Count").alias("Total_Medals"))
df_successful_countries.orderBy("Year", "Total_Medals", ascending=False).show(10)



# Athlete Career Longevity (First & Last Olympics)
df_athlete_career = df.groupBy("Athlete").agg(
    first("Year").alias("First_Olympics"),
    last("Year").alias("Last_Olympics"),
    countDistinct("Year").alias("Total_Olympics")
)
df_athlete_career.orderBy("Total_Olympics", ascending=False).show(10)

# Most Common Age for Winning Medals
df_medal_age = df.filter(df.Medal.isNotNull()).groupBy("Age").agg(count("Medal").alias("Medals_Won"))
df_medal_age.orderBy("Medals_Won", ascending=False).show(10)

# Most Competitive Sports (Highest Number of Athletes)
df_competitive_sports = df.groupBy("Sport").agg(countDistinct("Athlete").alias("Total_Athletes"))
df_competitive_sports.orderBy("Total_Athletes", ascending=False).show(10)

# Athlete Participation Trends Over Decades
df_decade_trend = df.withColumn("Decade", (df["Year"] / 10).cast("int") * 10)
df_decade_trend = df_decade_trend.groupBy("Decade").agg(countDistinct("Athlete").alias("Total_Athletes"))
df_decade_trend.orderBy("Decade").show()

# Gender Participation Trends Over Time
df_gender_trend = df.groupBy("Year", "Sex").agg(countDistinct("Athlete").alias("Total_Athletes"))
df_gender_trend.orderBy("Year").show(10)

# Most Successful Athletes by Medals per Sport
df_sport_athlete_medals = df.groupBy("Sport", "Athlete").agg(count("Medal").alias("Medals_Won"))
df_sport_athlete_medals.orderBy("Sport", "Medals_Won", ascending=False).show(10)


