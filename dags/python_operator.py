from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args={
    'owner': 'bn',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def greet_from_return(age, task_instance):
    name = task_instance.xcom_pull(task_ids="getNameRet")
    print("Hello! I am " + name + ", and I am " + age + " years old")

def greet_from_pull_xcom(task_instance):
    name = task_instance.xcom_pull(task_ids="getNameXcomPush", key="name")
    age = task_instance.xcom_pull(task_ids="getNameXcomPush", key="age")
    print("Hello! I am " + name + ", and I am " + age + " years old")

def getNameRet(n):
    return n

def getNameXcomPush(n, a, task_instance):
    task_instance.xcom_push(key="name", value=n)
    task_instance.xcom_push(key="age", value=a)

with DAG(
    default_args=default_args,
    dag_id='python_operator_v_1.5',
    description='Airflow DAG with Bash Operator',
    schedule_interval='@daily',
    start_date=datetime(2024, 2, 29, 2),
) as dag:
    task1=PythonOperator(
        task_id="greet_from_return",
        python_callable=greet_from_return,
        op_kwargs={"age":"32"}
    )

    task2=PythonOperator(
        task_id="getNameRet",
        python_callable=getNameRet,
        op_kwargs={"n": "Biswarup"}
    )

    task3=PythonOperator(
        task_id="greet_from_pull_xcom",
        python_callable=greet_from_pull_xcom
    )

    task4=PythonOperator(
        task_id="getNameXcomPush",
        python_callable=getNameXcomPush,
        op_kwargs={"n": "Biswarup", "a": "32.5"}
    )

    task2 >> task1
    task4 >> task3