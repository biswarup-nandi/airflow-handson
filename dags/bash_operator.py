from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bn',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='bash_operator_v_1.1',
    description='Airflow DAG with Bash Operator',
    default_args=default_args,
    start_date=datetime(2024, 2, 29, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='t1',
        bash_command="echo Hi From Task 1!"
    )

    task2 = BashOperator(
        task_id='t2',
        bash_command="echo Hi From Task 2!"
    )

    task3 = BashOperator(
        task_id='t3',
        bash_command="echo Hi From Task 3!"
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)