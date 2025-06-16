from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_db_data():
    # DB에서 데이터 조회하는 로직 작성
    pass

def branch_func(**context):
    # fetch_db_data 결과에 따라 분기 처리 (성공/실패)
    # 예) return 'success_task' or 'fail_task'
    pass

def success_task():
    # 성공 시 처리할 작업
    pass

def fail_task():
    # 실패 시 처리할 작업
    pass

def success_notification():
    # 성공 알림 보내는 로직
    pass

def fail_notification():
    # 실패 알림 보내는 로직
    pass

with DAG(
    dag_id='get_db_data_dag',
    start_date=datetime(2025, 6, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['db', 'etl', 'notification'],
) as dag:

    start = EmptyOperator(task_id='start')

    fetch = PythonOperator(
        task_id='fetch_db_data',
        python_callable=fetch_db_data,
    )

    branch = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_func,
        provide_context=True,
    )

    success = PythonOperator(
        task_id='success_task',
        python_callable=success_task,
    )

    fail = PythonOperator(
        task_id='fail_task',
        python_callable=fail_task,
    )

    success_notify = PythonOperator(
        task_id='success_notification',
        python_callable=success_notification,
    )

    fail_notify = PythonOperator(
        task_id='fail_notification',
        python_callable=fail_notification,
    )

    end = EmptyOperator(task_id='end')

    start >> fetch >> branch >> [success, fail]
    success >> success_notify >> end
    fail >> fail_notify >> end

