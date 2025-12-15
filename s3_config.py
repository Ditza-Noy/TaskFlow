# aws_config.py
import boto3
import os
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient

load_dotenv()

class SQSConfig:
    def __init__(self,
                 region_name: str | None = None,
                 access_key: str | None = None,
                 secret_key: str | None = None,
                 bucket_name: str | None = None):
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME")

        if not self.region_name:
            raise ValueError("AWS_REGION must be provided either as an argument or environment variable.")
        if not self.access_key:
            raise ValueError("AWS_ACCESS_KEY_ID must be provided either as an argument or environment variable.")
        if not self.secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY must be provided either as an argument or environment variable.")   
        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME must be provided either as an argument or environment variable.")
        
        self.s3_client : S3Client  = self._get_s3_client()
        self.bucket_url: str = self.get_s3_url(self.bucket_name)
        
    def _get_s3_client(self) -> S3Client:
        """Get configured S3 client."""        
        # We remove 'cast' because Pylance already knows it's S3Client.
        # We add the ignore comment to stop it from complaining about the dynamic factory definition.
        return boto3.client( # pyright: ignore[reportUnknownMemberType]
            's3',
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        
    def get_s3_url(self, bucket_name: str) -> str:
        """Get the URL of the specified S3 bucket."""
        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            return response['LocationConstraint']
        except self.s3_client.exceptions.NoSuchBucket:
            raise ValueError(f"S3 Bucket '{bucket_name}' does not exist.")