
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {
    'start_date': days_ago(2),
}

with DAG('first_dag', schedule_interval='@daily', default_args=args) as dag:
    b = BashOperator(task_id='simple_command', bash_command="echo 'It is my first DAG'")