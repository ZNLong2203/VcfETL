import os
import json
import psycopg2
import tempfile
from cyvcf2 import VCF
from airflow import DAG
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, exceptions, helpers
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Producer, Consumer, KafkaException

# Define the function to produce messages to Kafka
def produce_message(**kwargs):
    producer = Producer({
        'bootstrap.servers': 'broker:29092',
    })

    def delivery_report(err, msg):
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    vcf_file_path = kwargs['vcf_file']

    try:
        with open(vcf_file_path, 'r') as file:
            vcf_content = file.read()

        producer.produce(
            'vcf-topic',
            key='vcf',
            value=vcf_content,
            callback=delivery_report
        )
        producer.flush()
    except Exception as e:
        print(f"Error producing message: {e}")

# Define the function to create tables
def create_table(**kwargs):
    conn = psycopg2.connect(
        host="postgres",
        database="vcf",
        user="postgres",
        password="test"
    )
    cur = conn.cursor()

    try:
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
                variant_id VARCHAR(255),
                sample_id VARCHAR(255),
                allelic_depth VARCHAR(255),
                allele_frequency FLOAT,
                genotype VARCHAR(255),
                FOREIGN KEY(variant_id) REFERENCES variant(id),
                FOREIGN KEY(sample_id) REFERENCES sample(id),
                UNIQUE(variant_id, sample_id)
            );
        """)
        conn.commit()
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        cur.close()
        conn.close()

# Define the function to insert variants into PostgreSQL
def insert_variant_postgres(**kwargs):
    conn = psycopg2.connect(
        host="postgres",
        database="vcf",
        user="postgres",
        password="test"
    )
    cur = conn.cursor()

    vcf_file = kwargs['vcf_file']
    vcf = VCF(vcf_file)

    try:
        for variant in vcf:
            chrom, pos, ref, alt = variant.CHROM, variant.POS, variant.REF, variant.ALT
            qual, filter, info = variant.QUAL, variant.FILTER, dict(variant.INFO)
            if variant.ID is None:
                variant.ID = f'{variant.CHROM}-{variant.POS}-{variant.REF}-{variant.ALT}'

            cur.execute("""
                INSERT INTO variant(id, name, chrom, pos, ref, alt, qual, filter)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
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
                    ON CONFLICT (variant_id, sample_id) DO NOTHING
                """, (variant.ID, sample, ad_values, af_values, gt_values))
                conn.commit()
    except Exception as e:
        print(f"Error inserting variants: {e}")
    finally:
        cur.close()
        conn.close()

# Define the function to insert variants into Elasticsearch
def insert_variant_elasticsearch():
    conf = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'vcf-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    es = Elasticsearch([{'host': 'es_master', 'port': 9200}])
    consumer = Consumer(conf)
    consumer.subscribe(['vcf-topic'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            vcf_data = msg.value().decode('utf-8')

            # Write the VCF data to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmpfile:
                tmpfile.write(vcf_data)
                tmpfile_path = tmpfile.name

            # Now read from the temporary file
            try:
                vcf = VCF(tmpfile_path)
                variant_actions = []
                sample_actions = {}

                for variant in vcf:
                    variant_id = variant.ID if variant.ID else f'{variant.CHROM}-{variant.POS}-{variant.REF}-{variant.ALT}'
                    variant_dict = {
                        '_index': 'variants',
                        '_id': variant_id,
                        '_source': {
                            'variant_id': variant_id,
                            'chrom': variant.CHROM,
                            'pos': variant.POS,
                            'ref': variant.REF,
                            'alt': variant.ALT,
                            'qual': variant.QUAL,
                            'filter': variant.FILTER,
                            'info': dict(variant.INFO),
                            'samples': []
                        }
                    }
                    for i, sample in enumerate(vcf.samples):
                        sample_id = sample
                        allelic_depth = ','.join(map(str, variant.format('AD')[i])).replace(',', ' ')
                        allele_frequency = ','.join(map(str, variant.format('AF')[i]))
                        genotype = variant.genotypes[i][0]

                        sample_info = {
                            "sample_id": sample_id,
                            "allelic_depth": allelic_depth,
                            "allele_frequency": allele_frequency,
                            "genotype": genotype
                        }
                        variant_dict['_source']['samples'].append(sample_info)

                        if sample_id not in sample_actions:
                            sample_actions[sample_id] = {
                                '_index': 'samples',
                                '_id': sample_id,
                                '_source': {
                                    'sample_id': sample_id,
                                    'variants': []
                                }
                            }
                        sample_actions[sample_id]['_source']['variants'].append({
                            'variant_id': variant_id,
                            'chrom': variant.CHROM,
                            'pos': variant.POS,
                            'ref': variant.REF,
                            'alt': variant.ALT,
                            'allelic_depth': allelic_depth,
                            'allele_frequency': allele_frequency,
                            'genotype': genotype
                        })

                    variant_actions.append(variant_dict)

                if variant_actions:
                    helpers.bulk(es, variant_actions)
                if sample_actions:
                    helpers.bulk(es, sample_actions.values())
            finally:
                os.remove(tmpfile_path)

    except KeyboardInterrupt:
        pass
    except KafkaException as e:
        print(f"Kafka exception: {e}")
    except exceptions.ConnectionError as e:
        print(f"Elasticsearch connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        consumer.close()



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
    task_id='insert_variant_task',
    python_callable=insert_variant_postgres,
    op_kwargs={'vcf_file': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.vep.vcf')},
    dag=dag,
)

task_insert_variant_elasticsearch = PythonOperator(
    task_id='insert_variant_elasticsearch_task',
    python_callable=insert_variant_elasticsearch,
    dag=dag,
)

task_produce_message >> task_create_table >> [task_insert_variant_postgres, task_insert_variant_elasticsearch]
