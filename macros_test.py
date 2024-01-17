from datetime import datetime
from airflow import DAG, macros
from airflow.operators.bash import BashOperator

with DAG(dag_id="macros_test", schedule_interval="@daily",
    start_date=datetime(2023, 6, 29), catchup=False) as dag:
        b = BashOperator(
            task_id="execution_date",
            bash_command="echo 'execution date : {{ ds }} modified by macros.ds_add to add 5 days : {{ macros.ds_add(ds, 5) }}'"
    )