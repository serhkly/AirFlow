from google.cloud import bigquery
from scrapy.crawler import CrawlerProcess
import scrapy
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

class RegionSpider(scrapy.Spider):
    name = "region_spider"
    start_urls = ['http://example.com/regions']
    def parse(self, response):
        pass # replace with your region extraction code

class CommunitySpider(scrapy.Spider):
    name = "community_spider"
    start_urls = ['http://example.com/communities']
    def parse(self, response):
        pass # replace with your communities extraction code

class ListingSpider(scrapy.Spider):
    name = "listing_spider"
    start_urls = ['http://example.com/listings']
    def parse(self, response):
        pass # replace with your listings extraction code

def run_spider(spider_class):
    process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
    process.crawl(spider_class)
    process.start()  
    process.settings.attributes['FEED_URI'].value = f"{spider_class.name}.json"

def save_to_bigquery(spider_class):
   project_id = 'your-gcp-project-id'
   dataset_id = 'your-dataset-id'
   table_id = f'{spider_class.name}'

   client = bigquery.Client(project=project_id)

   # Define schemas
   region_schema = [
       bigquery.SchemaField("region_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("region_name", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("extracted_date", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("modification_time", "TIMESTAMP", mode="REQUIRED"),
   ]

   community_schema = [
       bigquery.SchemaField("community_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("region_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("community_name", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
       bigquery.SchemaField("no_of_houses", "INTEGER", mode="NULLABLE"),
       bigquery.SchemaField("extracted_date", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("modification_time", "TIMESTAMP", mode="REQUIRED"),
   ]

   listing_schema = [
       bigquery.SchemaField("listing_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("community_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
       bigquery.SchemaField("no_of_bedrooms", "INTEGER", mode="NULLABLE"),
       bigquery.SchemaField("no_of_bathrooms", "INTEGER", mode="NULLABLE"),
       bigquery.SchemaField("extracted_date", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("modification_time", "TIMESTAMP", mode="REQUIRED"),
   ]

   # Save the data into new tables
   table_schemas = {
       'region_spider': region_schema,
       'community_spider': community_schema,
       'listing_spider': listing_schema,
   }

   dataset_ref = client.dataset(dataset_id)
   table_ref = dataset_ref.table(table_id)

   # Try to fetch the table or create it if it does not exist yet
   try:
       client.get_table(table_ref)
       print(f"Table {table_id} already exists.")
   except NotFound:
       schema = table_schemas[spider_class.name]
       table = bigquery.Table(table_ref, schema=schema)
       client.create_table(table)

   job_config = bigquery.LoadJobConfig()
   job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

   # Load data from the JSON file into the table
   filename = f"{spider_class.name}.json"
   with open(filename, "rb") as source_file:
       job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
   job.result()

   print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

default_args = {'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}
dag = DAG(
    dag_id='livabl.com_v2',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)
...
# Defining tasks in Airflow DAG for each spider
with dag:
    run_region_spider_task = PythonOperator(
        task_id='run_region_spider',
        python_callable=run_spider,
        op_kwargs={'spider_class': RegionSpider}
    )

    save_region_data_task = PythonOperator(
        task_id='save_region_data',
        python_callable=save_to_bigquery,
        op_kwargs={'spider_class': RegionSpider}
    )

    run_community_spider_task = PythonOperator(
        task_id='run_community_spider',
        python_callable=run_spider,
        op_kwargs={'spider_class': CommunitySpider}  
    )

    save_community_data_task = PythonOperator(
        task_id='save_community_data',
        python_callable=save_to_bigquery,
        op_kwargs={'spider_class': CommunitySpider}
    )

    run_listing_spider_task = PythonOperator(
        task_id='run_listing_spider',
        python_callable=run_spider,
        op_kwargs={'spider_class': ListingSpider} 
    )

    save_listing_data_task = PythonOperator(
        task_id='save_listing_data',
        python_callable=save_to_bigquery,
        op_kwargs={'spider_class': ListingSpider}
    )

    # Defining task dependencies
    run_region_spider_task >> save_region_data_task
    run_community_spider_task >> save_community_data_task
    run_listing_spider_task >> save_listing_data_task



