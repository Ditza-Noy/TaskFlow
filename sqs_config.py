# aws_config.py
import boto3
import os
from dotenv import load_dotenv
from mypy_boto3_sqs import SQSClient

load_dotenv()

class SQSConfig:
    def __init__(self,
                 region_name: str | None = None,
                 access_key: str | None = None,
                 secret_key: str | None = None,
                 queue_name: str | None = None):
        self.region_name = region_name or os.getenv("AWS_REGION", "eu-west-1")
        self.access_key = access_key or os.environ.get("AWS_ACCESS_KEY_ID")
        self.secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.queue_name = queue_name or os.getenv("SQS_QUEUE_NAME", 'taskflow-queue')

        if not self.region_name:
            raise ValueError("AWS_REGION must be provided either as an argument or environment variable.")
        if not self.access_key:
            raise ValueError("AWS_ACCESS_KEY_ID must be provided either as an argument or environment variable.")
        if not self.secret_key:
            raise ValueError("AWS_SECRET_ACCESS_KEY must be provided either as an argument or environment variable.")   
        if not self.queue_name:
            raise ValueError("SQS_QUEUE_NAME must be provided either as an argument or environment variable.")
        
        self.sqs_client : SQSClient = self.get_sqs_client()
        self.queue_url: str = self.get_queue_url()
        
    def get_sqs_client(self) -> SQSClient:
        """Get configured SQS client."""        
        # We remove 'cast' because Pylance already knows it's SQSClient.
        # We add the ignore comment to stop it from complaining about the dynamic factory definition.
        return boto3.client( # pyright: ignore[reportUnknownMemberType]
            'sqs',
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        
    # def get_sqs_url(self, queue_name: str) -> str:
    #     """Get the URL of the specified SQS queue."""
    #     try:
    #         response = self.sqs_client.get_queue_url(QueueName=queue_name)
    #         return response['QueueUrl']
    #     except self.sqs_client.exceptions.QueueDoesNotExist:
    #         raise ValueError(f"SQS Queue '{queue_name}' does not exist.")

    def get_queue_url(self) -> str:
        """Get or create SQS queue URL."""
        sqs = self.get_sqs_client()
        try:
            # Try to get existing queue
            response = sqs.get_queue_url(QueueName=self.queue_name)
            return response['QueueUrl']
        except sqs.exceptions.QueueDoesNotExist:
        # Create new queue if it doesn't exist
            response = sqs.create_queue(QueueName=self.queue_name,
                    Attributes={
                    'VisibilityTimeout': '300', # 5 minutes
                    'MessageRetentionPeriod': '1209600', # 14 days
                    'ReceiveMessageWaitTimeSeconds': '10' # Long polling
                    }
                )
            return response['QueueUrl']