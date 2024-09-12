import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def upload_to_s3(file, bucket_name, s3_path):
    s3_client = boto3.client('s3')

    try:
        response = s3_client.upload_file(file, bucket_name, s3_path)
        print(f"{file} uploaded successfully to s3: {s3_path}")
    except Exception as e:
        print(f"Error in uploading file: \n{str(e)}")


upload_to_s3("./data/yellow_tripdata_2024-06.parquet", "taxitrip-datalake", \
                "yellowtaxi_tripdata/yellow_tripdata_2024-06.parquet")