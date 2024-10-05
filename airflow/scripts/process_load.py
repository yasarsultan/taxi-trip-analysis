import os
from google.cloud import bigquery
from google.oauth2 import service_account
from glob import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()
current_dir = os.getcwd()

def process(month):
    # Initialize SparkSession
    spark = SparkSession.builder.appName("NYCTaxiDataPipeline") \
            .config("fs.defaultFS", "file:///") \
            .getOrCreate()

    # Read Data
    df = spark.read.parquet(f"{current_dir}/data/taxitrip_data.parquet")

    # Rename columns
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

    # Data Cleaning
    imp_columns = ["pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance", "fare_amount"]
    cleaned_df = df.dropna(subset=imp_columns)

    cleaned_df = cleaned_df.filter(
        (F.month(F.col("pickup_datetime")) == month) &
        (F.month(F.col("dropoff_datetime")) == month) &
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
    
    transformed_df.repartition(1).write.parquet(f"{current_dir}/staging/taxitrip_data.parquet", mode="overwrite")
    
    # Close SparkSession
    spark.stop()
    
    # return [current_dir, month, f"{current_dir}/staging/taxitrip_data.parquet"]


def load():
    project_id = os.getenv("PROJECT_ID")
    table_id = os.getenv("TABLE_ID")

    file_path = glob(f"{current_dir}/staging/taxitrip_data.parquet/*.parquet")[0]
    
    try:
        credentials = service_account.Credentials.from_service_account_file(f"{current_dir}/service-account-file.json")
        client = bigquery.Client(credentials=credentials, project=project_id)

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        with open(file_path, "rb") as parquet_file:
            job = client.load_table_from_file(parquet_file, table_id, job_config=job_config)
            job.result()

        print("Data successfully loaded to BigQuery!")

    except Exception as e:
        print(f"Error in loading data to BigQuery: \n{str(e)}")

    # return [project_id, table_id, file_path] # This should only work when there is file in staging folder
