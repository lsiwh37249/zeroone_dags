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
dim_customer_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_customer.csv")
dim_product_updated_file_path = os.path.join(OLAP_DIR, "dim", "dim_product.csv")
fact_order_updated_file_path = os.path.join(OLAP_DIR, "fact", "fact_order.csv")

Modeling = OlapModeling()

def load(**context):
    log_df = Modeling.load()
    log_df.to_csv(log_df_file_path, index=False)

def dim_customer(**context):
    #dim_customer_updated = Modeling.dimension_customer(log_df_file_path,dim_customer_updated_file_path)
    updated_customers = Modeling.update_dimension_table(
        log_df_path=log_df_file_path,
        dim_file_path= dim_customer_updated_file_path,
        keys=['customer_name', 'region'],
        id_column_name='customer_id'
    )   

def dim_product(**context):
    #dim_product_updated = Modeling.dimension_product(log_df_file_path,dim_product_updated_file_path)
    updated_products = Modeling.update_dimension_table(
        log_df_path=log_df_file_path,
        dim_file_path=dim_product_updated_file_path,
        keys=['product_name', 'category'],
        id_column_name='product_id'
    )

def fact(**context):
    fact_order_updated = Modeling.fact(
        log_df_file_path,
        dim_product_updated_file_path,
        dim_customer_updated_file_path,
        fact_order_updated_file_path
    )
    fact_order_updated.to_csv(fact_order_updated_file_path, index=False)

def save(**context):
    Modeling.save(
        dim_customer_updated_file_path,
        dim_product_updated_file_path,
        fact_order_updated_file_path
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

    dimension_product = PythonOperator(
        task_id='dimension_product',
        python_callable=dim_product,
    )
    
    dimension_customer = PythonOperator(
        task_id='dimension_customer',
        python_callable=dim_customer,
    )

    fact = PythonOperator(
        task_id='fact',
        python_callable=fact,
    )

    save = PythonOperator(
        task_id='save',
        python_callable=save,
    )

    #skip_fact_and_save = PythonOperator(
    #    task_id='skip_fact_and_save',
    #    python_callable=lambda: print("⚠️ Skipping fact/save due to invalid data"),
    #)

    notification = PythonOperator(
        task_id='notification',
        python_callable=notification,
    )

    end = DummyOperator(task_id='end')

    # DAG 흐름 정의
    start >> load >> [dimension_customer, dimension_product] >> fact >> save >> notification >> end
