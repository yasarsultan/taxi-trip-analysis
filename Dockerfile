# Use the official Airflow image as a parent image
FROM apache/airflow:latest

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /dataPipeline

# Copy files into working directory
COPY . /dataPipeline

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Make the airflow setup script is executable
RUN chmod +x airflow_setup.sh

# Set the entrypoint
ENTRYPOINT ["airflow_setup.sh"]