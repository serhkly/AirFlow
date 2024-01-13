from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def _hello_world():
    logging.info('Hello')


    args = {
        'start_date': datetime(2024,1,7)
    }

    with DAG('test', schedule_interval="@daily", default_args=args) as dag:
        task_1 = PythonOperator(
            task_id='python_task',
            python_callable=_hello_world
        )
        
