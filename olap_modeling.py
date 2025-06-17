from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
import random  # 실제 검증 로직 대신 시뮬레이션용
from utils.OlapModeling import OlapModeling 
import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
OLAP_DIR = os.path.join(AIRFLOW_HOME, "data/olap")
os.makedirs(OLAP_DIR, exist_ok=True)

log_df_file_path = os.path.join(OLAP_DIR, "log_df.csv")
dim_event_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_event.csv")

Modeling = OlapModeling()

def load(**context):
    log_df = Modeling.load()
    log_df.to_csv(log_df_file_path, index=False)

def dim_event(**context):
    date = "20250601"
    id_column_name = "event_id"
    keys = ['event']
    Modeling.update_dimension_table(log_df_file_path, dim_event_updated_file_path, keys, id_column_name)

def fact(**context):
    pass
def save(**context):
    pass
def notification():
    print("notification")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='olap_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['olap'],
) as dag:


    start = DummyOperator(task_id='start')
    
    load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    dimension_event = PythonOperator(
        task_id='dimension_event',
        python_callable=dim_event,
    )

    fact = PythonOperator(
        task_id='fact',
        python_callable=fact,
    )

    save = PythonOperator(
        task_id='save',
        python_callable=save,
    )

    notification = PythonOperator(
        task_id='notification',
        python_callable=notification,
    )

    end = DummyOperator(task_id='end')

    # DAG 흐름 정의
    start >> load >> [dimension_event] >> fact >> save >> notification >> end
