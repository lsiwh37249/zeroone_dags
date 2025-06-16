from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta, datetime
import random  # 실제 검증 로직 대신 시뮬레이션용

# ------------------------------
def send_slack_alert(**context):
    print("[ALERT] DAG 실패 감지됨!")

# ------------------------------
def validate_data(**kwargs):
    result = random.choice(['valid', 'invalid'])  # 실제 검증 로직 대체
    kwargs['ti'].xcom_push(key='validation_result', value=result)
    print(f"[VALIDATION] result = {result}")

# ------------------------------
# Branch 로직
def choose_branch(**kwargs):
    result = kwargs['ti'].xcom_pull(task_ids='validate_data', key='validation_result')
    if result == 'valid':
        return 'dimension'
    else:
        return 'skip_fact_and_save'

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


    start = EmptyOperator(task_id='start')
    
    load = PythonOperator(
        task_id='load',
        python_callable=lambda: print("🔄 Loading data..."),
    )

    save_rawdata = PythonOperator(
        task_id='save_rawdata',
        python_callable=lambda: print("💾 Saving raw data..."),
    )

    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    branch = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=choose_branch,
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

    skip_fact_and_save = PythonOperator(
        task_id='skip_fact_and_save',
        python_callable=lambda: print("⚠️ Skipping fact/save due to invalid data"),
    )

    notification = PythonOperator(
        task_id='notification',
        python_callable=lambda: print(" slack notification "),
    )

    end = EmptyOperator(task_id='end')

    # DAG 흐름 정의
    start >> load >> save_rawdata >> validate_data >> branch
    branch >> dimension >> fact >> save >> notification >> end
    branch >> skip_fact_and_save >> notification >> end

