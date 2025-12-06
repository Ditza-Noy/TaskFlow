# sqs_queue.py
import json
import uuid
import boto3
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
from dataclasses import asdict
from task_queue import Task, TaskStatus, TaskQueue as BaseTaskQueue
from aws_config import AWSConfig
from base_task_queue import BaseTaskQueue
from mypy_boto3_sqs import SQSClient
from aws_config import AWSConfig


logger = logging.getLogger(__name__)

class SQSTaskQueue(BaseTaskQueue):
    """SQS-backed implementation of TaskQueue interface."""
    def __init__(self):
        super().__init__() # Initialize parent class attributes
        self.aws_config = AWSConfig()
        self.sqs : SQSClient = self.aws_config.sqs_client
        self.queue_url = self.aws_config.queue_url
        logger.info(f"SQS Queue initialized: {self.queue_url}")
        
    def enqueue(self, name: str, priority: int, payload: dict[str,str]) -> str:
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            name=name,
            priority=priority,
            payload=payload,
            status=TaskStatus.PENDING,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        response = self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody="Order #1234 details",
            MessageGroupId="OrderGroup-1234",  # Ensures strict ordering within this group
            MessageDeduplicationId="unique-id-1234" # Optional if content-based deduplication is on
        )
    
    def _task_to_sqs_message(self, task: Task) -> Dict[str, Any]:
        """Convert Task to SQS message format."""
        return {
        'id': task.id,
        'name': task.name,
        'priority': task.priority,
        'payload': task.payload,
        'status': task.status.value,
        'created_at': task.created_at.isoformat(),
        'updated_at': task.updated_at.isoformat()
        }
    def _sqs_message_to_task(self, message: Dict[str, Any]) -> Task:
        """Convert SQS message back to Task object."""
        body = json.loads(message['Body'])
        return Task(
            id=body['id'],
            name=body['name'],
            priority=body['priority'],
            payload=body['payload'],
            status=TaskStatus(body['status']),
            created_at=datetime.fromisoformat(body['created_at']),
            updated_at=datetime.fromisoformat(body['updated_at'])
        )
    