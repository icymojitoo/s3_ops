import os
from dotenv import load_dotenv
import boto3

# Load credentials
load_dotenv()

def create_s3_client(endpoint_url, aws_access_key_id, aws_secret_access_key, region_name):
    s3_client = boto3.client('s3',
        endpoint_url = endpoint_url,
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        region_name = region_name
    )
    return s3_client