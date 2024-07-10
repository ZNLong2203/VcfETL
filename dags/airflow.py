import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
import os
from cyvcf2 import VCF


# Define the function to create tables
def create_table(**kwargs):
    conn = psycopg2.connect(
        host="postgres",
        database="vcf",
        user="postgres",
        password="test"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS variant(
            id TEXT PRIMARY KEY,
            name VARCHAR(255),
            chrom VARCHAR(255),
            pos VARCHAR(255),
            ref VARCHAR(255),
            alt VARCHAR(255),
            qual FLOAT,
            filter VARCHAR(255),
            info JSONB
        );

        CREATE TABLE IF NOT EXISTS sample(
            id TEXT PRIMARY KEY,
            name VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS format(
            id serial PRIMARY KEY,
            variant_id TEXT,
            sample_id TEXT,
            allelic_depth TEXT,
            allele_frequency FLOAT,
            genotype VARCHAR(255),
            FOREIGN KEY(variant_id) REFERENCES variant(id),
            FOREIGN KEY(sample_id) REFERENCES sample(id)
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


# Define the function to insert variants
def insert_variant(**kwargs):
    conn = psycopg2.connect(
        host="postgres",
        database="vcf",
        user="postgres",
        password="test"
    )
    cur = conn.cursor()

    vcf_file = kwargs['vcf_file']
    vcf = VCF(vcf_file)

    for variant in vcf:
        chrom, pos, ref, alt = variant.CHROM, variant.POS, variant.REF, variant.ALT
        qual, filter, info = variant.QUAL, variant.FILTER, dict(variant.INFO)
        if variant.ID is None:
            cur.execute("SELECT COUNT(*) FROM variant;")
            variant.ID = str(cur.fetchone()[0] + 1)

        cur.execute("""
            INSERT INTO variant(id, name, chrom, pos, ref, alt, qual, filter)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        """, (variant.ID, variant.ID, chrom, pos, ref, alt, qual, filter))
        conn.commit()

        for i, sample in enumerate(vcf.samples):
            cur.execute("""
                INSERT INTO sample(id, name)
                VALUES(%s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (sample, sample))
            conn.commit()

            ad_values = ','.join(map(str, variant.format('AD')[i])).replace(',', ' ')
            af_values = ','.join(map(str, variant.format('AF')[i]))
            gt_values = variant.genotypes[i][0]

            cur.execute("""
                INSERT INTO format(variant_id, sample_id, allelic_depth, allele_frequency, genotype)
                VALUES(%s, %s, %s, %s, %s)
            """, (variant.ID, sample, ad_values, af_values, gt_values))
            conn.commit()

    cur.close()
    conn.close()


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

# Define the DAG
dag = DAG(
    'my_insert_data_dag',
    default_args=default_args,
    description='Insert data from VCF file into PostgreSQL',
    schedule_interval=timedelta(days=1),
)

# Define tasks in the DAG using PythonOperator
task_create_table = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    dag=dag,
)

task_insert_variant = PythonOperator(
    task_id='insert_variant_task',
    python_callable=insert_variant,
    op_kwargs={'vcf_file': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.vep.vcf')},
    dag=dag,
)

# Set task dependencies
task_create_table >> task_insert_variant
