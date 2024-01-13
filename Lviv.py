# Import necessary modules
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import json

# Define the DAG
with DAG(
    dag_id="first_dag_modified_lviv_v0", 
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True
) as dag:

    # Define the BashOperator task
    b = BashOperator(
        task_id="echo_hello",
        bash_command="echo '{{ execution_date }}'"
    )

    db_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS measures
            (
                timestamp TIMESTAMP,
                temp FLOAT
            );
        """
    )


    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn",
        endpoint="data/2.5/weather",
        request_params={
            "appid": Variable.get("WEATHER_API_KEY"),
            "q": "Lviv"
        }
    )


    extract_data = SimpleHttpOperator(
        task_id="extract_data",
        http_conn_id="weather_conn",
        endpoint="data/2.5/weather",
        data={
            "appid": Variable.get("WEATHER_API_KEY"),
            "q": "Lviv"
        },
        method="GET",
        response_filter=lambda x: json.loads(x.text),
        log_response=True
    )

    def _process_weather(ti):
        info = ti.xcom_pull("extract_data")
        timestamp = info["dt"]
        temp = info["main"]["temp"]
        return timestamp, temp

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_weather
    )


    inject_data = SqliteOperator(
        task_id="inject_data",
        sqlite_conn_id="airflow_conn",
        sql="""
            INSERT INTO measures (timestamp, temp) VALUES
            (
                {{ti.xcom_pull(task_ids='process_data')[0]}},
                {{ti.xcom_pull(task_ids='process_data')[1]}}
            );
        """,
    )

    # Connect everything together
    db_create >> check_api >> extract_data >> process_data >> inject_data