from minio import Minio
from minio.error import S3Error

def upload_file_to_minio(file_path, bucket_name, object_name, minio_url, access_key, secret_key):
    # Create a client with the MinIO server playground, its access key and secret key.
    client = Minio(
        minio_url,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    # Make 'bucket_name' if not exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket '{bucket_name}' already exists")

    # Upload file
    try:
        client.fput_object(bucket_name, object_name, file_path)
        print(f"'{file_path}' is successfully uploaded as '{object_name}' to bucket '{bucket_name}'.")
    except S3Error as e:
        print(f"Error occurred: {e}")
