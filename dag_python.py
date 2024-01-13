from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

args = {'start_date': datetime(2024, 1, 1)}

def _hello_world():
    logging.info("Hello, World!!!")

with DAG(dag_id="simple_python_dag1", schedule_interval="@daily", default_args = args) as dag:
    hello_world = PythonOperator(
        task_id="python_task",
        python_callable=_hello_world
    )