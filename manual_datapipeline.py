import os
import requests
import boto3
from google.cloud import bigquery
from google.oauth2 import service_account
from glob import glob
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

def extract_load(url, file_name):
    bucket_name = os.getenv("DATA_LAKE_BUCKET")
    s3_path = f"{bucket_name}/{file_name}"

    try:
        response = requests.get(url)
    except Exception as e:
        print(f"Failed to download data from URL: {url} with error: \n{str(e)}")
        return

    if response.status_code == 200:
        with open("data/taxitrip_data.parquet", "wb") as file:
            file.write(response.content)

        s3_client = boto3.client('s3')
        try:
            s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=response.content)
            print("Data loaded successfully!")
        except Exception as e:
            print(f"Error in uploading file: \n{str(e)}")
    else:
        print(f"Failed to download data from URL: {url} with status code: {response.status_code}")



def extract(spark):
    df = spark.read.parquet("data/taxitrip_data.parquet")

    return df

def transform(df, month):
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
    
    return transformed_df


def load(df):
    project_id = os.getenv("PROJECT_ID")
    table_id = os.getenv("TABLE_ID")

    df.repartition(1).write.parquet("staging/taxitrip_data.parquet", mode="overwrite")

    file_path = glob("staging/taxitrip_data.parquet/*.parquet")[0]
    try:
        credentials = service_account.Credentials.from_service_account_file("service-account-file.json")
        client = bigquery.Client(credentials=credentials, project=project_id)

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        with open(file_path, "rb") as parquet_file:
            job = client.load_table_from_file(parquet_file, table_id,
                                                job_config=job_config)
            job.result()
        print("Data loaded to BigQuery successfully!")

    except Exception as e:
        print(f"Error in loading data to BigQuery: \n{str(e)}")


def main():
    print("ETL Pipeline Started")
    
    # Extract and Load Data to Data Lake
    year, month = str(datetime.now().year), str(datetime.now().month - 2).zfill(2)
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

    extract_load(url, file_name)

    # Start SparkSession
    spark = SparkSession.builder.appName("NYCTaxiDataPipeline") \
        .config("fs.defaultFS", "file:///") \
        .getOrCreate()
    
    
    # Extract Data
    print("Extracting Data")
    dataframe = extract(spark)

    # Transform Data
    print("Transforming Data")
    transformed_df = transform(dataframe, month)

    # Load Data
    print("Loading Data")
    load(transformed_df)

    # Stop SparkSession
    spark.stop()

    print("ETL Pipeline Completed")

main()