# aws_config.py
import boto3
import os
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client
from botocore.exceptions import ClientError
from typing import cast
from mypy_boto3_s3.literals import BucketLocationConstraintType
from mypy_boto3_s3.type_defs import BucketLifecycleConfigurationTypeDef

load_dotenv()

class S3Config:
    def __init__(self,
                 region_name: str | None = None,
                 access_key: str | None = None,
                 secret_key: str | None = None,
                 bucket_name: str | None = None):
        self.region_name = region_name or os.getenv("AWS_REGION", "eu-west-1")
        self.access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "taskflow-bucket")

        if not self.region_name:
            raise ValueError("AWS_REGION must be provided either as an argument or environment variable.")
        if not self.access_key:
            raise ValueError("AWS_ACCESS_KEY_ID must be provided either as an argument or environment variable.")
        if not self.secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY must be provided either as an argument or environment variable.")   
        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME must be provided either as an argument or environment variable.")
        
        self.s3_client : S3Client  = self.get_s3_client()
        # self.bucket_url: str = self.get_s3_url(self.bucket_name)
        
    def get_s3_client(self) -> S3Client:
        """Get configured S3 client."""        
        # We remove 'cast' because Pylance already knows it's S3Client.
        # We add the ignore comment to stop it from complaining about the dynamic factory definition.
        return boto3.client( # pyright: ignore[reportUnknownMemberType]
            's3',
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
    
    def create_bucket_if_not_exists(self) -> bool:
        """Create S3 bucket if it doesn't exist."""
        s3 = self.get_s3_client()
        try:
            # Check if bucket exists
            s3.head_bucket(Bucket=self.bucket_name)
            print(f"S3 bucket '{self.bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = 0
            if 'Error' in e.response and 'Code' in e.response['Error']:
                error_code = int(e.response['Error']['Code'])
            if error_code == 404:
            # Bucket doesn't exist, create it
                try:
                    if self.region_name == 'us-east-1':
                        s3.create_bucket(Bucket=self.bucket_name)
                    else:
                        s3.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration={'LocationConstraint': cast(BucketLocationConstraintType, self.region_name)})
                        print(f"Created S3 bucket: {self.bucket_name}")
                    return True
                except ClientError as create_error:
                    print(f"Error creating bucket: {create_error}")
                    return False
            else:
                print(f"Error accessing bucket: {e}")
                return False
    def setup_bucket_lifecycle(self):
            """Set up lifecycle policies for cost optimization."""
            s3 = self.get_s3_client()            
            lifecycle_config: BucketLifecycleConfigurationTypeDef = {
                'Rules': [
                    {
                        'ID': 'TaskFlowLifecycle',
                        'Status': 'Enabled',
                        'Filter': {'Prefix': 'tasks/'},
                        'Transitions': [
                            {
                                'Days': 30,
                                'StorageClass': 'STANDARD_IA'
                            },
                            {
                                'Days': 90,
                                'StorageClass': 'GLACIER'
                            }
                        ]
                    }
                ]
            }

            try:
                s3.put_bucket_lifecycle_configuration(
                    Bucket=self.bucket_name,
                    LifecycleConfiguration=lifecycle_config
                )
                print("S3 lifecycle policies configured")
            except ClientError as e:
                print(f"Error setting lifecycle policies: {e}")
        
    def get_s3_url(self, bucket_name: str) -> str:
        """Get the URL of the specified S3 bucket."""
        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            return response['LocationConstraint']
        except self.s3_client.exceptions.NoSuchBucket:
            raise ValueError(f"S3 Bucket '{bucket_name}' does not exist.")