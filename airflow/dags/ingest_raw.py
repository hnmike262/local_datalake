from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-eng',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='ingest_raw_data',
    default_args=default_args,
    schedule_interval='@weekly',  # Run weekly
    catchup=False,
    tags=['raw-ingestion'],
    description='Ingest raw CSV data into MinIO raw bucket',
) as dag:
    
    # Example: Ingest customers CSV
    ingest_customers = BashOperator(
        task_id='ingest_customers',
        bash_command=(
            'echo "TODO: Implement CSV ingestion to MinIO raw bucket" && '
            'echo "This would normally copy files from /opt/data/raw to MinIO"'
        ),
    )
    
    # Example: Ingest orders CSV
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command=(
            'echo "TODO: Implement CSV ingestion to MinIO raw bucket" && '
            'echo "This would normally copy files from /opt/data/raw to MinIO"'
        ),
    )
    
    # Run in parallel
    [ingest_customers, ingest_orders]
