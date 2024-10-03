import os
import requests
from boto3 import client
from dotenv import load_dotenv

load_dotenv()

def extractLoad(file_name):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    bucket_name = os.getenv("DATA_LAKE_BUCKET")
    s3_path = f"{bucket_name}/{file_name}"

    try:
        response = requests.get(url)
    except Exception as e:
        print(f"Failed to download data from URL: {url} with error: \n{str(e)}")
        return

    if response.status_code == 200:
        current_dir = os.getcwd()
        with open(f"{current_dir}/data/taxitrip_data.parquet", "wb") as file:
            file.write(response.content)

        s3_client = client('s3')
        try:
            s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=response.content)
            print("Data loaded successfully!")
        except Exception as e:
            print(f"Error in uploading file: \n{str(e)}")
    else:
        print(f"Failed to download data from URL: {url} with status code: {response.status_code}")
    
    # return [url, bucket_name, s3_path]
