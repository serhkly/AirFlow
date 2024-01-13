# Import required modules
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# Define default arguments for your DAG
args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
}
# Define your DAG
with DAG(
    'skeleton',
    description='A simple DAG',
    schedule_interval='0 12 * * *',
    default_args=args,
    catchup=False,
) as dag:
    # Define tasks/operators for your DAG
    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')
    # Define task dependencies
    task_1 >> task_2 >> task_3
