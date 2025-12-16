from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-eng',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='medallion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['medallion', 'transformations'],
    description='Bronze → Silver → Gold transformation pipeline via dbt',
) as dag:
    
    # dbt Bronze: Load raw data into Bronze views
    dbt_bronze = BashOperator(
        task_id='dbt_bronze',
        bash_command=(
            'cd /opt/dbt && '
            'dbt run --select path:models/bronze --profiles-dir . --target docker'
        ),
    )
    
    # dbt Silver: Transform Bronze into Silver tables
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command=(
            'cd /opt/dbt && '
            'dbt run --select path:models/silver --profiles-dir . --target docker'
        ),
    )
    
    # dbt Gold: Aggregate Silver into Gold tables
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command=(
            'cd /opt/dbt && '
            'dbt run --select path:models/gold --profiles-dir . --target docker'
        ),
    )
    
    # Task dependencies: Bronze → Silver → Gold
    dbt_bronze >> dbt_silver >> dbt_gold
