import os
from confluent_kafka import Producer

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
