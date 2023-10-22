import time
import os
import sys
from dotenv import load_dotenv
from config.s3 import create_s3_client
from utils.buckets import BucketManager

# Load credentials
load_dotenv()

s3_client = create_s3_client(
       endpoint_url=os.environ.get('BUCKET_ENDPOINT'),
       aws_access_key_id=os.environ.get('BUCKET_ACCESS_KEY'),
       aws_secret_access_key=os.environ.get('BUCKET_SECRET_ACCESS_KEY'),
       region_name=os.environ.get('BUCKET_REGION_NAME')
)

FILENAME = ''
DESTINATION = ''

def download_file():
    bucket_manager = BucketManager(s3_client, os.environ.get('BUCKET_NAME'))
    bucket_manager.download_file(FILENAME, DESTINATION)

if __name__ == '__main__':
       download_file()