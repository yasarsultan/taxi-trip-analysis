# Use the official Airflow image as a parent image
FROM apache/airflow:latest

# Set the working directory
WORKDIR /dataPipeline

# Copy files into working directory
COPY . /dataPipeline

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Switch user to modify file permissions
USER root

# Make the airflow setup script is executable
RUN chmod +x /dataPipeline/airflow_setup.sh

# Switch back to normal user
USER airflow

# Expose airflow webserver port so that it can be accessible in local machine web browser
EXPOSE 8080

# Set the entrypoint
ENTRYPOINT ["/dataPipeline/airflow_setup.sh"]
CMD ["bash"]