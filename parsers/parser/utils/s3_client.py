import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
ENDPOINT = os.getenv('MINIO_ENDPOINT_URL', 'http://minio:9000')
BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', 'raw-data')

s3_client = boto3.client(
    's3',
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4')
)

def upload_file_to_s3(file_path: str, object_name: str) -> bool:
    try:
        try:
            s3_client.head_bucket(Bucket=BUCKET_NAME)
        except ClientError:
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            print(f"Бакет {BUCKET_NAME} создан.")

        s3_client.upload_file(file_path, BUCKET_NAME, object_name)
        return True
    except Exception as e:
        print(f"Ошибка загрузки {file_path} в S3: {e}")
        return False