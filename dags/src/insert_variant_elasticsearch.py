import os
import tempfile
import time
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch, exceptions, helpers
from cyvcf2 import VCF

def insert_variant_elasticsearch(max_duration=20):
    conf = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'vcf-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    es = Elasticsearch([{'host': 'es_master', 'port': 9200}])
    consumer = Consumer(conf)
    consumer.subscribe(['vcf-topic'])

    start_time = time.time()

    try:
        while time.time() - start_time < max_duration:
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

    except KafkaException as e:
        print(f"Kafka exception: {e}")
    except exceptions.ConnectionError as e:
        print(f"Elasticsearch connection error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        consumer.close()
