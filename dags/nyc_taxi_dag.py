"""
NYC Taxi Airflow DAG
====================
Orchestrates the NYC Taxi ETL pipeline dynamically.
Demonstrates Airflow Backfilling, Logical Dates, and Jinja Templating.
"""

import os # FIX: Added to allow merging with existing system environment variables
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator # FIX: Updated import for Airflow 3
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_student',
    'depends_on_past': False, # Allows runs to happen independently
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'nyc_taxi_etl_pipeline',
    default_args=default_args,
    description='Sequential ETL pipeline for NYC Taxi Data',
    schedule='@monthly',   # Runs once per month logically
    start_date=datetime(2024, 1, 1), # Start at Jan 2024
    end_date=datetime(2024, 4, 1),   # Stop at April 2024 (Prevents infinite future loops)
    catchup=True,                    # CRITICAL: Tells Airflow to run all past missed months
    max_active_runs=1,               # CRITICAL: Forces runs to happen one at a time sequentially
) as dag:

    # 1. logical_date.strftime("%Y-%m") dynamically becomes "2024-01", then "2024-02", etc.
    # 2. 'sleep 180' pauses the worker for 3 minutes after the Python script finishes.
    #    This gives students time to watch the UI and check the database before the next month starts.
    run_etl_task = BashOperator(
        task_id='run_etl_script',
        bash_command='python /opt/airflow/scripts/etl_pipeline.py {{ logical_date.strftime("%Y-%m") }} && sleep 180',
        # FIX: Provide the database credentials directly to the python script
        env={
            "DATABASE_URL": "postgresql://airflow:airflow@postgres:5432/airflow",
            **os.environ # Keeps existing Airflow variables intact
        }
    )

    run_etl_task