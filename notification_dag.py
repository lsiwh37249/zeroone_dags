from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

def read_and_alert_error():
    try:
        with open('/tmp/error_log.txt', 'r') as f:
            error = f.read()
    except FileNotFoundError:
        error = "에러 로그 없음"
    
    print(f"[🔔 ERROR 알림] {error}")
    # 실제 운영에서는 Slack API 호출 등 추가

with DAG(
    dag_id='notification_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_transform = ExternalTaskSensor(
        task_id='wait_for_transform_dag',
        external_dag_id='transform_dag',
        external_task_id='trigger_notification_dag',
        timeout=600,
        mode='poke',
    )

    notify = PythonOperator(
        task_id='read_and_notify',
        python_callable=read_and_alert_error,
    )

    wait_transform >> notify

