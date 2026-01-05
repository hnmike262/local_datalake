from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 12, 25),
}

with DAG(
    dag_id='pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL pipeline',
) as dag:
    
    # Load raw data into Bronze 
    dbt_bronze = BashOperator(
        task_id='dbt_bronze',
        bash_command=(
            'cd /opt/dbt && '
            'dbt run --select path:models/bronze --profiles-dir . --target docker --threads 1'
        ),
    )
    
    # Transform Bronze into Silver 
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command=(
            'cd /opt/dbt && '
            'dbt run --select path:models/silver --profiles-dir . --target docker --threads 1'
        ),
    )
    
    # Aggregate Silver into Gold 
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command=(
            'cd /opt/dbt && '
            'dbt run --select path:models/gold --profiles-dir . --target docker --threads 1'
        ),
    )
    
    # Bronze → Silver → Gold
    dbt_bronze >> dbt_silver >> dbt_gold
