# Using the minimal python image from the docker hub
FROM python:3.10-slim

# Creating a working directory for the data pipeline
WORKDIR /data-pipeline

# Copying the requirements file to the working directory
COPY requirements.txt .

# Installing the required packages for the data pipeline
RUN pip install --no-cache-dir -r requirements.txt

# Copying the data pipeline files to the working directory
COPY . .

# Running the ETL pipeline
CMD ["python", "etl.py"]

# Cleaning up the data to make the image lighter
RUN rm -rf data/taxitrip_data.parquet staging/taxitrip_data.parquet