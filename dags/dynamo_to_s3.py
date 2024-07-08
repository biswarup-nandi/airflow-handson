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
from airflow.providers.amazon.aws.transfers.dynamodb_to_s3 import DynamoDBToS3Operator
from airflow.utils.dates import datetime
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime,timedelta
import time

# Externally fetched variables:
DYNAMO_TBL = "dummyDynamoData"
S3_BUCKET = "testbkt120792"
S3_KEY_PREFIX = "dynamodb-data/json/"

default_args = {
    'owner': 'bn',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dynamo_to_s3_v_1.0.1',
    default_args=default_args,
    description='Airflow DAG to Transfer Data From DynamoDB to S3',
    schedule_interval='@daily',
    start_date=datetime(2024, 2, 29, 2)
) as dag:
    dynamodb_to_s3_job = DynamoDBToS3Operator(
        task_id="dynamo_to_S3",
        dynamodb_table_name=DYNAMO_TBL,
        s3_bucket_name=S3_BUCKET,
        s3_key_prefix=S3_KEY_PREFIX,
        export_format='DYNAMODB_JSON',
        source_aws_conn_id='aws-conn',
        dest_aws_conn_id='aws-conn',
        file_size=20,
    )

    dynamodb_to_s3_job

