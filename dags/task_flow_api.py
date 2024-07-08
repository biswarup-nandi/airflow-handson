from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'bn',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='task_flow_api_v_1.1',
    default_args=default_args,
    description='Airflow DAG with Task Flow API',
    schedule_interval='@daily',
    start_date=datetime(2024, 2, 29, 2)
)
def dummyEtl():

    @task()
    def get_name():
        return "BN"
    
    @task(multiple_outputs=True)
    def get_fname_lname():
        return {
            'fname': 'B',
            'lname': 'Nandi'
        }

    @task()
    def get_age():
        return "33"
    
    @task()
    def greet(name, age):
        print("Hello " + name + "! Your age is: " + age)

    @task()
    def greet_multi_param(fname, lname, age):
        print("Hello " + fname + " " + lname + "! Your age is: " + age)

    name = get_name()
    age = get_age()
    fname_lname = get_fname_lname()
    greet(name=name, age=age)
    greet_multi_param(fname=fname_lname['fname'], lname=fname_lname['lname'], age=age)


greet_dag = dummyEtl()