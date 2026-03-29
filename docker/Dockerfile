# Use the official Airflow image as our base
FROM apache/airflow:slim-3.1.8-python3.12

# Switch to the root user briefly to install any necessary system packages (if needed)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user to install Python dependencies securely
USER airflow

# Copy the requirements file into the container
COPY requirements.txt /requirements.txt

# Install our ETL dependencies (pandas, sqlalchemy, etc.) into the Airflow environment
RUN pip install --no-cache-dir -r /requirements.txt