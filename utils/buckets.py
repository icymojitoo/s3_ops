"""Bucket Management"""
import os
import logging
from botocore.exceptions import ClientError

class BucketManager:
    """
    Manage interactions with S3 bucket

    Attributes:
        s3_client: Boto3 S3 client
        bucket_name: Name of S3 bucket

    Methods:
        download_file: Download file from S3 bucket
        upload_file: Upload file to S3 bucket
        delete_file: Delete file from S3 bucket
        upload_dir: Upload directory to S3 bucket
        download_dir: Download directory from S3 bucket
        delete_dir: Delete directory from S3 bucket
    """
    def __init__(self, s3_client, bucket_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name

    def download_file(self, key, destination):
        # Download file from S3 bucket
        try:
            with open(destination, 'wb') as f:
                self.s3_client.download_fileobj(
                    self.bucket_name, 
                    key, 
                    f
                )
                # print('Download Successful')
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def upload_file(self, file_name, object_name):
        # Upload a file to an S3 bucket
        try:
            response = self.s3_client.upload_file(
                file_name, 
                self.bucket_name, 
                object_name
            )
            # print('Upload Successful')
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def delete_file(self, key):
        # Delete file from S3 bucket
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            print('Delete Successful')
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_dir(self, local_directory, s3_prefix):
        """Upload an entire directory to S3 bucket."""
        if not os.path.isdir(local_directory):
            raise ValueError("Please provide a valid directory path")

        for root, dirs, files in os.walk(local_directory):
            for filename in files:
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, local_directory)
                s3_path = os.path.join(s3_prefix, relative_path)
                self.upload_file(local_path, s3_path)

        # print(f"Upload of {local_directory} to S3 bucket {self.bucket_name} complete")
                
    def download_dir(self, s3_prefix, local_directory):
        """Download an entire directory from S3 bucket."""
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=s3_prefix):
            for content in result.get("Contents", []):
                key = content.get("Key")
                local_path = os.path.join(local_directory, key[len(s3_prefix):].lstrip('/'))
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                self.download_file(key, local_path)

        # print(f"Download of {s3_prefix} from S3 bucket {self.bucket_name} complete")
                
    def delete_dir(self, s3_prefix):
        """Delete an entire directory from S3 bucket."""
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=self.bucket_name, Prefix=s3_prefix):
            for content in result.get("Contents", []):
                key = content.get("Key")
                self.delete_file(key)

        # print(f"Delete of {s3_prefix} from S3 bucket {self.bucket_name} complete")
