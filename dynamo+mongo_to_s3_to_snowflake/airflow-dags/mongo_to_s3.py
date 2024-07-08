# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.mongo_to_s3 import MongoToS3Operator
from airflow.utils.dates import datetime
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime,timedelta
import time

# Externally fetched variables:
MONGO_DATABASE = "admin"
MONGO_COLLECTION = "dummyData"
S3_BUCKET = "testbkt120792"
S3_KEY = "mongo-data/json/" + str(time.time()) + ".json"

default_args = {
    'owner': 'bn',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='mongo_to_s3_v_1.0.1',
    default_args=default_args,
    description='Airflow DAG to Transfer Data From MongoDB to S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 2, 29, 2)
) as dag:
    mongo_to_s3_job = MongoToS3Operator(
        task_id="mongo_to_s3_job",
        mongo_conn_id='ec2-mongo',
        aws_conn_id = 'aws-conn',
        mongo_db=MONGO_DATABASE,
        mongo_collection=MONGO_COLLECTION,
        mongo_query={"status": "OK"},
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        replace=True
    )

    mongo_to_s3_job

