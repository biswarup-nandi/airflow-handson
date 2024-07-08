from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bn',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='catchup_backfill_v_1.1',
    default_args=default_args,
    description='Airflow DAG with Catchup and Backfill',
    schedule_interval='@daily',
    start_date=datetime(2024, 2, 29, 2),
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id='t1',
        bash_command="echo Hi From Task 1!"
    )