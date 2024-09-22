from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("NYCTaxiTransformation") \
    .config("fs.defaultFS", "file:///") \
    .getOrCreate()


df = spark.read.parquet("data/yellow_tripdata_2024-06.parquet")
# Load data from S3
# df = spark.read.paquet("s3a://taxitrip-datalake/...")

# Rename columns
df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
               .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")


# Data Cleaning
imp_columns = ["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "fare_amount"]
cleaned_df = df.dropna(subset=imp_columns)

cleaned_df = cleaned_df.filter(
    (F.month(F.col("pickup_datetime")) == 6) &
    (F.month(F.col("dropoff_datetime")) == 6) &
    (F.col("pickup_datetime") < F.col("dropoff_datetime")) &
    (F.col("passenger_count") > 0) & (F.col("passenger_count") < 6) &
    (F.col("trip_distance") > 0) & (F.col("trip_distance") < 100) &
    (F.col("fare_amount") > 0) & (F.col("fare_amount") < 500)
)


# Data Transformation
transformed_df = cleaned_df \
    .withColumn("trip_duration_min", F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2)) \
    .withColumn("trip_distance", F.round(F.col("trip_distance") * 1.60934, 2)) \
    .withColumn("hour_of_day", F.hour(F.col("pickup_datetime"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("pickup_datetime"))) \
    .withColumn("month", F.month(F.col("pickup_datetime"))) \
    .withColumn("year", F.year(F.col("pickup_datetime")))



transformed_df.show(5)
transformed_df.write.parquet("tripdata_transformed.parquet")


# Stop SparkSession
spark.stop()