import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from src.minio_utils import find_new_vcf_file, download_vcf_from_minio, delete_vcf_from_minio
from src.produce_message import produce_message
from src.create_table import create_table
from src.insert_variant_postgres import insert_variant_postgres
from src.insert_variant_elasticsearch import insert_variant_elasticsearch

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_insert_data_dag',
    default_args=default_args,
    description='Insert data from VCF file into PostgreSQL and Elasticsearch',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

local_vcf_file_path = '/tmp/'

sensor_task = ShortCircuitOperator(
    task_id='sensor_task',
    python_callable=find_new_vcf_file,
    op_kwargs={
        'bucket_name': 'landingzone'
    },
    provide_context=True,
    dag=dag,
)

def download_and_process(**context):
    bucket_name = context['params']['bucket_name']
    vcf_file_name = context['ti'].xcom_pull(task_ids='sensor_task')
    if not vcf_file_name:
        raise ValueError("No VCF file found in MinIO bucket.")
    local_file_path = os.path.join(local_vcf_file_path, vcf_file_name)
    download_vcf_from_minio(bucket_name, vcf_file_name, local_file_path)
    return local_file_path

task_download_vcf = PythonOperator(
    task_id='download_vcf_task',
    python_callable=download_and_process,
    params={'bucket_name': 'landingzone'},
    provide_context=True,
    dag=dag,
)

task_produce_message = PythonOperator(
    task_id='produce_message_task',
    python_callable=produce_message,
    op_kwargs={'vcf_file': '{{ ti.xcom_pull(task_ids="download_vcf_task") }}'},
    provide_context=True,
    dag=dag,
)

task_create_table = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    dag=dag,
)

task_insert_variant_postgres = PythonOperator(
    task_id='insert_variant_postgres_task',
    python_callable=insert_variant_postgres,
    op_kwargs={'vcf_file': '{{ ti.xcom_pull(task_ids="download_vcf_task") }}'},
    provide_context=True,
    dag=dag,
)

task_insert_variant_elasticsearch = PythonOperator(
    task_id='insert_variant_elasticsearch_task',
    python_callable=insert_variant_elasticsearch,
    op_kwargs={'max_duration': 20},
    dag=dag,
)

def delete_processed_vcf(**context):
    bucket_name = context['params']['bucket_name']
    vcf_file_name = context['ti'].xcom_pull(task_ids='sensor_task')
    delete_vcf_from_minio(bucket_name, vcf_file_name)

task_delete_vcf = PythonOperator(
    task_id='delete_vcf_task',
    python_callable=delete_processed_vcf,
    params={'bucket_name': 'landingzone'},
    provide_context=True,
    dag=dag,
)

sensor_task >> task_download_vcf >> task_create_table >> task_insert_variant_postgres >> task_delete_vcf
sensor_task >> task_download_vcf >> task_produce_message >> task_insert_variant_elasticsearch >> task_delete_vcf

