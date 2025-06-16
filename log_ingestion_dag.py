from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import os
from utils.BigQueryFetcher import BigQueryFetcher

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

BQ_PROJECT = 'your-gcp-project-id'
BQ_DATASET = 'staging'
BQ_TABLE = 'raw_logs'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_logs():
    fetcher = BigQueryFetcher()
    #fetcher.get_data()
    fetcher.make_temp_data()
def check_data_condition():
    # 예시: 데이터 수집 결과에 따라 분기
    # True면 success, False면 fail로 분기
    # 실제 조건에 맞게 수정하세요
    data_exists = True  # 임의 변수
    if data_exists:
        return 'success'
    else:
        return 'fail'

with DAG(
    dag_id='log_ingestion_dag',
    start_date=datetime(2025, 6, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['log', 'ingestion', 'bigquery'],
) as dag:


    start = DummyOperator(task_id='start')

    fetch = PythonOperator(
        task_id='fetch_logs',
        python_callable=fetch_logs
    )

    branch = BranchPythonOperator(
        task_id='branching',
        python_callable=check_data_condition
    )

    success = DummyOperator(task_id='success')
    fail = DummyOperator(task_id='fail')


    end = DummyOperator(task_id='end')

    start >> fetch >> branch >> [success, fail] >> end

