from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'nyc_taxi_etl_k8s_pipeline',
    default_args=default_args,
    description='K8s native ETL pipeline for NYC Taxi Data',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 4, 1),
    catchup=True,
    max_active_runs=1,
) as dag:

    run_etl_task = KubernetesPodOperator(
        task_id='run_etl_container',
        # Spin the pod up in the default namespace right next to our Postgres database!
        namespace='default', 
        image='my-k8s-etl:latest',
        image_pull_policy='IfNotPresent',
        cmds=["python", "/app/scripts/etl_pipeline.py"],
        # Jinja Templating: Passes the dynamic month to your Python script (e.g. "2024-01")
        arguments=["{{ logical_date.strftime('%Y-%m') }}"], 
        # Inject the database connection directly
        env_vars={'DATABASE_URL': 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'},
        name='taxi-etl-pod',
        get_logs=True, # Pulls the Python print() statements into the Airflow UI
        is_delete_operator_pod=True, # Cleans up the "dead body" container after it finishes!
    )