# Use an official lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the scripts directory into the container
COPY scripts/ scripts/

# The default command to run when the container starts
CMD ["python", "scripts/etl_pipeline.py"]