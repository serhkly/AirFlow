from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Functions for each task in the DAG
def extract_regions():
    # Code to extract regions 
    print('Regions extracted')

def extract_communities():
    # Code to extract communities
    print('Communities extracted')

def extract_listings():
    # Code to extract listings information
    print('Listing extracted')

def transform_data():
    # Code to transform and clean data
    print('Data transformed')

def load_data():
    # Code to upload clean data to database
    print('Data loaded')

# Default arguments
default_args = {
    'start_date':days_ago(1),
}

# Define a DAG
with DAG(
    dag_id='livabl_data_pipeline',
    default_args=default_args,
    description='A data pipeline that extracts data from Livabl',
    schedule_interval='0 10 * * *',
)as dag:
    t1 = PythonOperator(task_id='extract_regions', python_callable=extract_regions)
    t2 = PythonOperator(task_id='extract_communities', python_callable=extract_communities)
    t3 = PythonOperator(task_id='extract_listings', python_callable=extract_listings)
    t4 = PythonOperator(task_id='transform_data', python_callable=transform_data)
    t5 = PythonOperator(task_id='load_data', python_callable=load_data)

# Setting up dependencies among tasks
t1 >> t2 >> t3 >> t4 >> t5 

