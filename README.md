# NYC Taxi Trip Batch Data Pipeline

## Project Overview
This project implements an automated batch data pipeline for processing NYC yellow taxi trip data. It leverages Apache Spark for data transformation, Airflow for workflow orchestration, Docker for containerization, and integrates cloud services (AWS S3 and Google Cloud BigQuery) for data storage and analytics.

## Key Features
- **Automated Data Processing**: Monthly ingestion and processing of more than 3 million NYC taxi trip records.
- **Cloud Integration**: Utilizes AWS S3 as a data lake for raw data storage and Google Cloud BigQuery as a data warehouse for transformed data.
- **Containerized Workflow**: Entire Airflow environment containerized using Docker for consistent deployment across environments.
- **Scalable Architecture**: Designed to handle increasing data volumes with minimal configuration changes.
- **Data Quality Checks**: Implements data validation to ensure the integrity of the data.

## Architecture

![img1](https://github.com/yasarsultan/taxi-trip-analysis/blob/main/imgs/taxi-datapipeline1.png)

The pipeline follows this high-level architecture:
1. **Data Extraction**: Raw data is fetched from the NYC Taxi & Limousine Commission's website.
2. **Data Lake Storage**: Raw data is stored in AWS S3 for durability and easy access.
3. **Data Transformation**: Apache Spark is used to clean and transform the data.
4. **Data Warehouse Loading**: Transformed data is loaded into Google Cloud BigQuery for downstream use-cases like analytics and machine learning.
5. **Workflow Orchestration**: Apache Airflow manages the entire workflow, ensuring tasks are executed in the correct order and on schedule.

![img2](https://github.com/yasarsultan/taxi-trip-analysis/blob/main/imgs/dag.png)

## Project Structure
- This project follows modular approach
```
batch-datapipeline/
│
├── airflow/                          # Directory for Airflow DAGs (workflows)
│   └── scripts
|       ├── dag.py                    # Main dag for the data pipeline
|       ├── extract_load.py           # Script to extract and load raw data
|       └── process_load.py           # Core script of the data pipeline
|
├── data/                             # Raw data stored locally
│   └── taxitrip_data.parquet
│
├── staging/                          # Transformed data stored locally 
│   └── taxitrip_data.parquet/           
│
├── airflow_setup.sh                  # Bash script to setup airflow environment
├── manual_datapipeline.py            # Python script to run data pipeline manually with more flexibility
├── .dockerignore                     # Files and directories ignored by Docker
├── .gitignore                        # Files and directories ignored by Git
├── Dockerfile                        # Dockerfile to containerize the project
├── README.md                         # Project documentation (this file)
├── requirements.txt                  # Python dependencies for the project
```

## Installation

### Prerequisites
- Docker
- AWS Account with S3 access
- Google Cloud Account with BigQuery access

### Steps
1. Clone the repository:
   ```
   git clone https://github.com/yasarsultan/taxi-trip-analysis.git
   cd taxi-trip-analysis
   ```

2. Set up environment:
   - Create a `.env` file in the project root and add your AWS and Google Cloud credentials:
   ```
   AWS_ACCESS_KEY_ID=your_aws_access_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   DATA_LAKE_BUCKET=your_s3_bucket_name
   PROJECT_ID=google_cloud_project_id
   DATASET_ID=bigquery_dataset_id
   TABLE_ID=bigquery_table_id
   ```
   - Add Google Cloud credentials file in project directory with name ```service-account-file.json```

3. Build the Docker image:
   ```
   docker build -t nyc-taxitrip-datapipeline .
   ```

## Usage
### Approach 1:
1. Start the Airflow container:
   ```
   docker run -p 8080:8080 nyc-taxi-datapipeline
   ```

2. Access the Airflow web interface at `http://localhost:8080`. Use the following credentials:
   - Username: admin
   - Password: admin

3. In the Airflow UI, enable the `batch_datapipeline` DAG.

4. The pipeline will run on monthly basis, processing the data of two months ago. You can also trigger a manual run from the Airflow UI.

### Approach 2: 
- *If you wish to run data pipeline manually without Airflow and Docker, follow these steps:*

1. **Ensure Installation is Complete**: Verify that you have successfully completed first and second installation steps.
2. **Install Dependencies**: Run the following command to install all required packages: `pip install -r requirements.txt`
3. **Execute the Script**: Once the above steps are completed, you can initiate the data pipeline by executing the `manual_datapipeline.py` file.

## Challenges and Solutions

1. **Data Source**: Extracting data from the [source](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) required a specific link to the latest available dataset. I Initially attempted to solve this by using web scraping.
- Solution: But when I was exploring the source site for web scraping, I discovered a pattern in the URL structure that involved a minor change in the link. After recognizing this pattern, I utilized the datetime library to solve this issue, which streamlined the process of automated data extraction.

2. **Data Transfer to BigQuery**: I faced difficulties in efficiently transferring data from a Spark DataFrame to BigQuery. My initial approach involved using the Spark-BigQuery Connector; however, its requirement for Google Cloud Storage posed access limitations.
- Solution: After further exploration, I identified a [direct method](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) but encountered [restrictions](https://cloud.google.com/bigquery/docs/sandbox) with stream loading in the free tier. To overcome this, I implemented a workaround by storing data locally as Parquet file and utilized the [BigQuery Connection API](https://cloud.google.com/python/docs/reference/bigquery/latest) for Python to load this file in append mode, effectively resolving the data transfer issue.

3. **Data Quality**: For removing outliers, I initially opted for the Interquartile Range (IQR) method but experienced performance issues when executing it, that too for just one column.
- Solution: I opted to hard-code the range for outlier removal as a quick fix to avoid the resource-intensive and time-consuming IQR method. While this solution worked, it wasn't ideal and left room for improvement in terms of scalability and efficiency.