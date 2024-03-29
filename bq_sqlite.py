from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pandas as pd

# Connect to the SQLite db
engine = create_engine('sqlite:////home/plemya/airflow/airflow.db')

default_args = {
    'owner': 'airflow',
}

def df_to_sqlite(df):
     # Load the df to SQLite db
    df.result().to_gbq('from_bq', engine, if_exists='replace', index=False)
    print("Data loaded to sqlite successfully!")

dag = DAG(
    'example_bq_to_sqlite',
    default_args=default_args,
    description='A BigQuery to SQLite DAG',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

fetch_data = BigQueryGetDataOperator(
    task_id='fetch_data',
    dataset_id='bigquery-public-data.samples',
    table_id='shakespeare',
    max_results='1000',
    selected_fields='word',
    dag=dag
)

store_data = PythonOperator(
    task_id='store_data',
    python_callable=df_to_sqlite,
    op_args=[fetch_data.output],
    dag=dag,
 )

fetch_data >> store_data
