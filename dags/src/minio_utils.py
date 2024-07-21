from minio import Minio
from airflow.hooks.base_hook import BaseHook
import logging

def get_minio_client():
    conn = BaseHook.get_connection('minio_default')
    return Minio(
        conn.host,
        access_key=conn.login,
        secret_key=conn.password,
        secure=False
    )

def find_new_vcf_file(bucket_name):
    client = get_minio_client()
    objects = client.list_objects(bucket_name)
    for obj in objects:
        logging.info(f"Found object: {obj.object_name}")
        if obj.object_name.endswith('.vcf'):
            logging.info(f"Found VCF file: {obj.object_name}")
            return obj.object_name
    logging.warning("No VCF file found in MinIO bucket.")
    return None

def download_vcf_from_minio(bucket_name, vcf_file_name, local_file_path):
    client = get_minio_client()
    client.fget_object(bucket_name, vcf_file_name, local_file_path)

def delete_vcf_from_minio(bucket_name, vcf_file_name):
    client = get_minio_client()
    client.remove_object(bucket_name, vcf_file_name)

## Need to create connection in Airflow UI (http)
## Conn Id: minio_default
## Conn Type: HTTP
## Host: minio:9000
## Login: admin
## Password: admin123
## Port: 9000