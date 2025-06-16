from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy

# DB 연결 정보 (예시)
DB_CONN_STR = 'mysql+pymysql://user:password@host:3306/dbname'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_db_data():
    engine = sqlalchemy.create_engine(DB_CONN_STR)
    query = "SELECT * FROM your_table WHERE DATE(created_at) = CURDATE()"
    df = pd.read_sql(query, con=engine)
    print(f"Fetched {len(df)} rows from DB.")
    # 예: CSV로 저장하거나, S3 / GCS 업로드 작업을 할 수도 있음
    df.to_csv('/tmp/db_data.csv', index=False)

with DAG(
    dag_id='get_db_data_dag',
    start_date=datetime(2025, 6, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['db', 'etl'],
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_db_data',
        python_callable=fetch_db_data
    )

