import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from src.produce_message import produce_message
from src.create_table import create_table
from src.insert_variant_postgres import insert_variant_postgres
from src.insert_variant_elasticsearch import insert_variant_elasticsearch

# Define default arguments for the DAG
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

task_produce_message = PythonOperator(
    task_id='produce_message_task',
    python_callable=produce_message,
    op_kwargs={'vcf_file': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.vep.vcf')},
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
    op_kwargs={'vcf_file': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.vep.vcf')},
    dag=dag,
)

task_insert_variant_elasticsearch = PythonOperator(
    task_id='insert_variant_elasticsearch_task',
    python_callable=insert_variant_elasticsearch,
    dag=dag,
)

task_create_table >> task_produce_message >> [task_insert_variant_postgres, task_insert_variant_elasticsearch]
