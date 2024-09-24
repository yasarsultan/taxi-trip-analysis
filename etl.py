import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def extract(spark):
    df = spark.read.parquet("data/yellow_tripdata_2024-06.parquet")

    return df

def transform(df):
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
        .withColumn("day_of_week", F.dayofweek(F.col("pickup_datetime")))
    
    return transformed_df



def load(df, table_id):
    bigquery_options = {
        "table": table_id,
        "writeMethod": "direct"
    }
    try:
        df.write \
            .format("bigquery") \
            .options(**bigquery_options) \
            .mode("overwrite") \
            .save()
        print("Data loaded to BigQuery")
    except Exception as e:
        print(f"Error in loading data to BigQuery: \n{str(e)}")

    df.repartition(1).write.parquet("staging_data/nyctaxi_data.parquet", mode="overwrite")


def main():
    print("Starting ETL Pipeline")
    # Load Environment Variables
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    project_id = os.environ["PROJECT_ID"]
    dataset_id = os.environ["DATASET_ID"]

    # Start SparkSession
    spark = SparkSession.builder.appName("NYCTaxiDataPipeline") \
        .config("fs.defaultFS", "file:///") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0") \
        .getOrCreate()
    
    spark.conf.set("parentProject", project_id)
    
    # Extract Data
    print("Extracting Data")
    data = extract(spark)

    # Transform Data
    print("Transforming Data")
    dataframe = transform(data)

    # Load Data
    print("Loading Data")
    load(dataframe, dataset_id)

    # Stop SparkSession
    spark.stop()

    print("ETL Pipeline Completed")

main()