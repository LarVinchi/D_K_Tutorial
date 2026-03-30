# Use the full, standard Airflow image (includes FAB and Postgres drivers out of the box)
FROM apache/airflow:3.1.8-python3.12

# Copy requirements and set correct permissions
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Define constraints for Airflow 3.1.8
ARG AIRFLOW_VERSION=3.1.8
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install custom dependencies safely
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    -r /opt/airflow/requirements.txt \
    --constraint "${CONSTRAINT_URL}"