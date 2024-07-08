from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta

default_args = {
    'owner': 'bn',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='mongo_to_s3_v_1.0',
    default_args=default_args,
    description='Airflow DAG with Task Flow API',
    schedule_interval='@daily',
    start_date=datetime(2024, 2, 29, 2)
)
def dummyEtl():
    @task()
    def get_data_from_mongo():
        mongo_conn = MongoHook(conn_id='ec2-mongo')
        collection = mongo_conn.get_collection('dummyData', 'admin')
        data =  list(collection.find())
        print(data)

    get_data_from_mongo()

etl_dag = dummyEtl()