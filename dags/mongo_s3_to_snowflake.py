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

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator
from airflow.utils.dates import datetime
from datetime import datetime,timedelta
import time

# Externally fetched variables:
aws_conn = BaseHook.get_connection("aws-conn")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password
SNOWFLAKE_CONN_ID = 'snowflake_conn'
SNOWFLAKE_SCHEMA = 'MONGO_AND_DYNAMO_DATA_SCHEMA'
SNOWFLAKE_STAGE = 'MONGO_AND_DYNAMO_DATA_STAGE'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'MONGO_AND_DYNAMO_DATA_DB'
SNOWFLAKE_STAGE_TABLE = 'STAGE_DATA_TBL'
SNOWFLAKE_MONGO_DATA_TABLE = 'MONGO_DATA_TBL'
SNOWFLAKE_FILE_FORMAT = 'JSON_FORMAT'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
S3_FILE_PATH = 's3://testbkt120792/mongo-data/json/'

SNOWSQL_DROP_STAGE_STATEMENT = f"DROP STAGE IF EXISTS {SNOWFLAKE_STAGE};"

SNOWSQL_CREATE_STAGE_STATEMENT = f'''CREATE OR REPLACE STAGE {SNOWFLAKE_STAGE}
                                    URL = '{S3_FILE_PATH}'
                                    CREDENTIALS = (
                                        AWS_KEY_ID = '{AWS_ACCESS_KEY}'
                                        AWS_SECRET_KEY = '{AWS_SECRET_KEY}'
                                    )
                                    FILE_FORMAT = {SNOWFLAKE_FILE_FORMAT};'''

SNOWSQL_COPY_STAGE_STATEMENT = f'''COPY INTO {SNOWFLAKE_STAGE_TABLE}
                                    FROM @{SNOWFLAKE_STAGE}
                                    FILE_FORMAT = (FORMAT_NAME = {SNOWFLAKE_FILE_FORMAT});'''

SNOWSQL_LOAD_MONGO_DATA_TABLE_STATEMENT = f'''MERGE INTO {SNOWFLAKE_MONGO_DATA_TABLE} AS target
                                                USING (
                                                    SELECT 
                                                        JSON_DATA:age::int AS AGE,
                                                        JSON_DATA:fullname::char(10) AS FULLNAME,
                                                        JSON_DATA:status::char(10) AS STATUS
                                                    FROM 
                                                        {SNOWFLAKE_STAGE_TABLE}
                                                ) AS source
                                                ON target.AGE = source.AGE AND target.FULLNAME = source.FULLNAME
                                                WHEN MATCHED THEN 
                                                    UPDATE SET
                                                        target.STATUS = source.STATUS
                                                WHEN NOT MATCHED THEN 
                                                    INSERT (AGE, FULLNAME, STATUS)
                                                    VALUES (source.AGE, source.FULLNAME, source.STATUS);'''

SNOWSQL_REMOVE_DATA_FROM_STAGE_STATEMENT = f"REMOVE @{SNOWFLAKE_STAGE};"

SNOWSQL_REMOVE_DATA_FROM_STAGE_TABLE_STATEMENT = f"TRUNCATE TABLE {SNOWFLAKE_STAGE_TABLE};"

default_args = {
    'owner': 'bn',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='mongo_s3_to_snowflake_v_1.0.1',
    default_args=default_args,
    description='Airflow DAG to Transfer Data From Mongo DB data in S3 to Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2024, 2, 29, 2)
) as dag:
    SNOWSQL_DROP_STAGE_STATEMENT_TASK = SnowflakeOperator(
        task_id="SNOWSQL_DROP_STAGE_STATEMENT_TASK",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        sql=SNOWSQL_DROP_STAGE_STATEMENT,
        role=SNOWFLAKE_ROLE
    )
    SNOWSQL_CREATE_STAGE_STATEMENT_TASK = SnowflakeOperator(
        task_id="SNOWSQL_CREATE_STAGE_STATEMENT_TASK",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        sql=SNOWSQL_CREATE_STAGE_STATEMENT,
        role=SNOWFLAKE_ROLE
    )
    SNOWSQL_COPY_STAGE_STATEMENT_TASK = SnowflakeOperator(
        task_id="SNOWSQL_COPY_STAGE_STATEMENT_TASK",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        sql=SNOWSQL_COPY_STAGE_STATEMENT,
        role=SNOWFLAKE_ROLE
    )
    SNOWSQL_LOAD_MONGO_DATA_TABLE_STATEMENT_TASK = SnowflakeOperator(
        task_id="SNOWSQL_LOAD_MONGO_DATA_TABLE_STATEMENT_TASK",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        sql=SNOWSQL_LOAD_MONGO_DATA_TABLE_STATEMENT,
        role=SNOWFLAKE_ROLE
    )
    SNOWSQL_REMOVE_DATA_FROM_STAGE_STATEMENT_TASK = SnowflakeOperator(
        task_id="SNOWSQL_REMOVE_DATA_FROM_STAGE_STATEMENT_TASK",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        sql=SNOWSQL_REMOVE_DATA_FROM_STAGE_STATEMENT,
        role=SNOWFLAKE_ROLE
    )
    SNOWSQL_REMOVE_DATA_FROM_STAGE_TABLE_STATEMENT_TASK = SnowflakeOperator(
        task_id="SNOWSQL_REMOVE_DATA_FROM_STAGE_TABLE_STATEMENT_TASK",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        schema=SNOWFLAKE_SCHEMA,
        sql=SNOWSQL_REMOVE_DATA_FROM_STAGE_TABLE_STATEMENT,
        role=SNOWFLAKE_ROLE
    )

    SNOWSQL_DROP_STAGE_STATEMENT_TASK >> SNOWSQL_CREATE_STAGE_STATEMENT_TASK >> SNOWSQL_COPY_STAGE_STATEMENT_TASK >> SNOWSQL_LOAD_MONGO_DATA_TABLE_STATEMENT_TASK >> SNOWSQL_REMOVE_DATA_FROM_STAGE_STATEMENT_TASK >> SNOWSQL_REMOVE_DATA_FROM_STAGE_TABLE_STATEMENT_TASK

