from google.cloud import bigquery
from google.cloud import storage
from scrapy.crawler import CrawlerProcess
import scrapy
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

#creation scrapy spider classes that defines how to crawl and extract structured data from website
class RegionSpider(scrapy.Spider):
    name = "region_spider"
    start_urls = ['http://livabl.com/austin/regions']
    def parse(self, response):
        pass # region extraction code

class CommunitySpider(scrapy.Spider):
    name = "community_spider"
    start_urls = ['http://livabl.com/austin/communities']
    def parse(self, response):
        pass # communities extraction code

class ListingSpider(scrapy.Spider):
    name = "listing_spider"
    start_urls = ['http://livabl.com/austin/listings']
    def parse(self, response):
        pass #listings extraction code

#initialization and run a crawl process for a given Scrapy spider
def run_spider(spider_class):
    process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'}) #initializes a CrawlerProcess
    process.crawl(spider_class) #runs the spider
    process.start() #starts the Scrapy engine
    process.settings.attributes['FEED_URI'].value = f"{spider_class.name}.json"  #sets the FEED_URI setting to a JSON file named after the spider class

    # Create storage client
    storage_client = storage.Client()
    # Name of the bucket
    bucket_name = 'my-bucket'
     # Name of the blob
    blob_name = f"{spider_class.name}.json"
    # Get the bucket that the file will be uploaded to
    bucket = storage_client.get_bucket(bucket_name)
    # Create a new blob and upload the file
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(blob_name)

#initializing a connection to Google BigQuery client
def save_to_bigquery(spider_class):
   project_id = 'gcp-project-id'  #specifying Google Cloud project ID
   dataset_id = 'dataset-id'  #ID of the BigQuery dataset to load data into
   table_id = f'{spider_class.name}'  #identifier for the BigQuery table to insert data into

   client = bigquery.Client(project=project_id)  #instance of a bigquery.Client() is used to communicate with the BigQuery API
    
    # Load data from GCS into BigQuery
   uri = f"gs://my-bucket-name/{spider_class.name}.json"  #sets the location JSON file in GCS to load into BigQuery.
   job_config = bigquery.LoadJobConfig()   #new job to import data into BigQuery from Cloud Storage.
   job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON   #sets the source format of the data to newline-delimited JSON
   job = client.load_table_from_uri(uri, table_id, job_config=job_config)  #load job that loads data from GCS bucket into the specified table in BigQuery
   job.result()

   print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

   # Defining schema for providing historical data analysis in future
   region_schema = [
       bigquery.SchemaField("region_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("region_name", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("extracted_date", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
    ]

   community_schema = [
       bigquery.SchemaField("community_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("region_id", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("community_name", "STRING", mode="REQUIRED"),
       bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
       bigquery.SchemaField("no_of_houses", "INTEGER", mode="NULLABLE"),
       bigquery.SchemaField("extracted_date", "TIMESTAMP", mode="REQUIRED"),
       bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
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
   ]

   # Save the data into new tables
   table_schemas = {
       'region_spider': region_schema,
       'community_spider': community_schema,
       'listing_spider': listing_schema,
   }
#Creates DatasetReference and TableReference for the BigQuery to interact with the referenced dataset
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

#establish the configuration for a load job into BigQuery from a variety of data sources
   job_config = bigquery.LoadJobConfig()
   job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON  # how BigQuery interprets the data to be imported

   # Load data from the JSON file into the table
   filename = f"{s.name}.json"   #creates a string for the filename
   with open(filename, "rb") as source_file:
       job = client.load_table_from_file(source_file, table_ref, job_config=job_config)   #starts a BigQuery load job
   job.result()   #waits for the job to complete

   print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

default_args = {'start_date':days_ago(1)}
with DAG(
    dag_id='livabl.com_v2',
    default_args=default_args,
    schedule_interval='0 10 * * *', # run each day at 10 am
)as dag:
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



