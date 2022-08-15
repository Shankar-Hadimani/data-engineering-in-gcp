import os
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingest_data_local_pgdb import ingest_data_callable, download_dataset


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL = 'https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet'
URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{  execution_date.strftime(\'%Y-%m\')  }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{  execution_date.strftime(\'%Y-%m\')  }}.parquet'
TABLE_NAME_TEMPLATE='/yellow_taxi_{{  execution_date.strftime(\'%Y_%m\')  }}'
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

local_workflow = DAG(
    "Local_Ingestion_DAG",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
)

with local_workflow:
    dummy_task = BashOperator(
        task_id='DUMMY_TASK',
        bash_command='echo "DUMMY BASH OPERATOR TASK....(..)!!!"'
    )

    wget_task = PythonOperator(
    task_id='WGET_FILES',
    python_callable=download_dataset,
    op_kwargs=dict(
            url=URL_TEMPLATE, 
            filepath=OUTPUT_FILE_TEMPLATE

    )
    )

    ingest_task = PythonOperator(
        task_id='INGEST_PGADMIN',
        python_callable=ingest_data_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            hostname=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            parquet_filename=OUTPUT_FILE_TEMPLATE
        ),
    )

    dummy_task >> wget_task >> ingest_task