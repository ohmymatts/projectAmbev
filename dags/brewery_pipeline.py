from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from scripts.api_client import BreweryApiClient
from scripts.data_processing import BreweryDataProcessor
from pyspark.sql import SparkSession
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def create_spark_session():
    return (SparkSession.builder
            .appName("BreweryDataPipeline")
            .config("spark.sql.shuffle.partitions", "5")
            .getOrCreate())

def extract_data(**kwargs):
    api_client = BreweryApiClient()
    breweries = api_client.get_all_breweries()
    kwargs['ti'].xcom_push(key='raw_breweries', value=breweries)

def process_bronze(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='raw_breweries')
    spark = create_spark_session()
    processor = BreweryDataProcessor(spark)
    
    output_path = Variable.get("data_lake_path")
    bronze_df = processor.create_bronze_layer(raw_data, output_path)
    kwargs['ti'].xcom_push(key='bronze_df_path', value=f"{output_path}/bronze")

def process_silver(**kwargs):
    spark = create_spark_session()
    processor = BreweryDataProcessor(spark)
    
    output_path = Variable.get("data_lake_path")
    bronze_path = kwargs['ti'].xcom_pull(task_ids='process_bronze', key='bronze_df_path')
    bronze_df = spark.read.json(bronze_path)
    
    silver_df = processor.create_silver_layer(bronze_df, output_path)
    kwargs['ti'].xcom_push(key='silver_df_path', value=f"{output_path}/silver")

def process_gold(**kwargs):
    spark = create_spark_session()
    processor = BreweryDataProcessor(spark)
    
    output_path = Variable.get("data_lake_path")
    silver_path = kwargs['ti'].xcom_pull(task_ids='process_silver', key='silver_df_path')
    silver_df = spark.read.parquet(silver_path)
    
    processor.create_gold_layer(silver_df, output_path)

with DAG('brewery_data_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         max_active_runs=1) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )
    
    bronze_task = PythonOperator(
        task_id='process_bronze',
        python_callable=process_bronze,
        provide_context=True
    )
    
    silver_task = PythonOperator(
        task_id='process_silver',
        python_callable=process_silver,
        provide_context=True
    )
    
    gold_task = PythonOperator(
        task_id='process_gold',
        python_callable=process_gold,
        provide_context=True
    )
    
    extract_task >> bronze_task >> silver_task >> gold_task