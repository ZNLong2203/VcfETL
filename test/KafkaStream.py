import psycopg2
from confluent_kafka import Producer, SerializingProducer
from cyvcf2 import VCF

if __name__ == '__main__':
    vcf_file = "test.vep.vcf"
    vcf = VCF(vcf_file)

    producer = Producer({
        'bootstrap.servers': 'localhost:9092',
    })

    producer.produce(
        'vcf-topic',
        key='vcf',
        value=vcf_file.encode('utf-8')
    )

    producer.flush()