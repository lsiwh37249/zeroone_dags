from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
from utils.OlapModeling import OlapModeling
from utils.BigQueryOlapModeling import BigQueryOlapModeling
from utils.BigQueryDimLoader import BigQueryDimLoader
import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/home/kim/app/airflow")
OLAP_DIR = os.path.join(AIRFLOW_HOME, "data/olap")

### 파일 저장 경로 : log 데이터, dim 데이터, fact 데이터
raw_path = ""
log_path = os.path.join(OLAP_DIR, "log")
dim_member_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_member.csv")
dim_event_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_event_type.csv")
dim_study_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_study.csv")
dim_date_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_date.csv")
dim_time_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_time.csv")
fact_updated_path = ""

### 클래스
Modeling = OlapModeling()
bq = BigQueryOlapModeling()
bq_dim_loader = BigQueryDimLoader()
def load(**context):
    date = context['execution_date'].strftime('%Y%m%d')
    raw_path = os.path.join(AIRFLOW_HOME, "data", "temp", f"event_log_{date}.csv")
    Modeling.load(date, raw_path, log_path)

"""
    keys : 기존에는 없는 값을 구분하기 위한 칼럼들
    id_column_name : dim 테이블의 기본키
"""

def dim_member(**context):
    keys = ['dl_member_id']
    id_column_name = 'member_id'
    date = context['execution_date'].strftime('%Y%m%d')
    log_path = os.path.join(OLAP_DIR, "log")
    log_path_with_date = os.path.join(log_path, date)
    Modeling.update_dimension_table(log_path_with_date,dim_member_updated_file_path,keys,id_column_name)

def dim_study(**context):
    keys = ['dl_study_id']
    id_column_name = 'study_id'
    date = context['execution_date'].strftime('%Y%m%d')
    log_path = os.path.join(OLAP_DIR, "log")
    log_path_with_date = os.path.join(log_path, date)
    Modeling.update_dimension_table(log_path_with_date, dim_study_updated_file_path, keys, id_column_name)

def dim_date(**context):
    keys = ['date']
    id_column_name = 'date_id'
    date = context['execution_date'].strftime('%Y%m%d')
    log_path = os.path.join(OLAP_DIR, "log")
    log_path_with_date = os.path.join(log_path, date)
    Modeling.update_dimension_table(log_path_with_date, dim_date_updated_file_path, keys, id_column_name)
    
def dim_time(**context):
    keys = ['time']
    id_column_name = 'time_id'
    date = context['execution_date'].strftime('%Y%m%d')
    log_path = os.path.join(OLAP_DIR, "log")
    log_path_with_date = os.path.join(log_path, date)
    Modeling.update_dimension_table(log_path_with_date, dim_time_updated_file_path, keys, id_column_name)

def dim_event(**context):
    keys = ['event']
    id_column_name = 'event_type_id'
    date = context['execution_date'].strftime('%Y%m%d')
    log_path = os.path.join(OLAP_DIR, "log")
    log_path_with_date = os.path.join(log_path, date)
    Modeling.update_dimension_table(log_path_with_date, dim_event_updated_file_path, keys, id_column_name)

def fact(**context):
    date = context['execution_date'].strftime('%Y%m%d')
    fact_updated_path = os.path.join(OLAP_DIR, "fact", f"fact_event_{date}")
    log_path = os.path.join(OLAP_DIR, "log")
    log_path_with_date = os.path.join(log_path, date)
    Modeling.fact(log_path_with_date,
           dim_member_updated_file_path,
           dim_event_updated_file_path,
           dim_date_updated_file_path,
           dim_time_updated_file_path,
           dim_study_updated_file_path,
           fact_updated_path)

def validation():
    pass

def upload_dims(**context):
    
    project_id = "zeroone"
    dataset_id = "test_dataset"
    dim_root = f"{AIRFLOW_HOME}/data/olap/dim"
    #date = context['execution_date'].strftime('%Y%m%d')
    bq_dim_loader.load(project_id, dataset_id, dim_root)

    
def upload_fact(**context):

    project_id = "zeroone"
    dataset_id = "test_dataset"
    staging_table = "staging_fact_event"
    fact_table = "fact_event"
    fact_root = f"{AIRFLOW_HOME}/data/olap/fact" 
    date = context['execution_date'].strftime('%Y%m%d')
    fact_root_with_date = os.path.join(fact_root,f"fact_event_{date}.csv")

    # -> 존재하면 굳이 넣을 필요 없는데 넣어지는 경우를 방지하기
    #bq.create_table(project_id,dataset_id,fact_table,"event_timestamp")
    bq.load_csv(
        fact_root_with_date,
        project_id, dataset_id,
        staging_table,                # staging 테이블
        "event_timestamp",
        "WRITE_TRUNCATE"
    )
    # 2단계: MERGE
    bq.merge_staging_to_fact(
        project_id, dataset_id,
        staging_table, fact_table     # 병합 대상 테이블
    )

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

    task_dim_member = PythonOperator(
        task_id='dim_member',
        python_callable=dim_member,
        provide_context=True,
    )

    task_dim_study = PythonOperator(
        task_id='dim_study',
        python_callable=dim_study,
        provide_context=True,
    )

    task_dim_event = PythonOperator(
        task_id='dim_event',
        python_callable=dim_event,
        provide_context=True,
    )

    task_dim_date = PythonOperator(
        task_id='dim_date',
        python_callable=dim_date,
        provide_context=True,
    )

    task_dim_time = PythonOperator(
        task_id='dim_time',
        python_callable=dim_time,
        provide_context=True,
    )

    task_fact = PythonOperator(
        task_id='fact',
        python_callable=fact,
        provide_context=True,
    )

    task_valid = PythonOperator(
        task_id='validation',
        python_callable=validation,
        provide_context=True,
    )

    task_upload_dims= PythonOperator(
        task_id='upload_dims',
        python_callable=upload_dims,
        provide_context=True,
    )

    task_upload_fact= PythonOperator(
        task_id='upload_fact',
        python_callable=upload_fact,
        provide_context=True,
    )


    notification = PythonOperator(
        task_id='notification',
        python_callable=notification,
    )

    end = DummyOperator(task_id='end')

    # DAG 흐름 정의
    start >> load >>  [
        task_dim_member,
        task_dim_study,
        task_dim_event,
        task_dim_date,
        task_dim_time
    ] >> task_fact  >> task_valid >> task_upload_dims >> task_upload_fact >> notification >> end
