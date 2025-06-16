from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
import random  # 실제 검증 로직 대신 시뮬레이션용
from utils.OlapModeling import OlapModeling

Modeling = OlapModeling()

def load():
    Modeling.load()

def dimension():
    Modeling.dimension()

def fact():
    Modeling.fact()


def send_slack_alert(**context):
    Modeling.send_slack_alert()


# ------------------------------
def save_error_to_file(**kwargs):
    with open('/tmp/error_log.txt', 'w') as f:
        f.write("에러: 데이터 처리 실패 - 테이블 불일치")
 
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

    dimension = PythonOperator(
        task_id='dimension',
        python_callable=lambda: print("📐 Processing dimension data..."),
    )

    fact = PythonOperator(
        task_id='fact',
        python_callable=lambda: print("📊 Processing fact data..."),
    )

    save = PythonOperator(
        task_id='save',
        python_callable=lambda: print("✅ Saving final data..."),
    )

    #skip_fact_and_save = PythonOperator(
    #    task_id='skip_fact_and_save',
    #    python_callable=lambda: print("⚠️ Skipping fact/save due to invalid data"),
    #)

    notification = PythonOperator(
        task_id='notification',
        python_callable=lambda: print(" slack notification "),
    )

    end = DummyOperator(task_id='end')

    # DAG 흐름 정의
    start >> load >> dimension >> fact >> save >> notification >> end

