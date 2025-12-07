# sqs_queue.py
import json
import uuid
import boto3
import botocore.exceptions
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
from dataclasses import asdict
from task_queue import Task, TaskStatus, TaskQueue as BaseTaskQueue
from aws_config import AWSConfig
from base_task_queue import BaseTaskQueue
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef
from aws_config import AWSConfig
import threading


logger = logging.getLogger(__name__)

class SQSTaskQueue(BaseTaskQueue):
    """SQS-backed implementation of TaskQueue interface."""
    def __init__(self):
        super().__init__() # Initialize parent class attributes
        self.aws_config = AWSConfig()
        self.sqs : SQSClient = self.aws_config.sqs_client
        self.queue_url = self.aws_config.queue_url
        # Task dictionary for O(1) status lookups and updates
        self._tasks: Dict[str, Task] = {}
        # Lock for protecting shared data (the queue and tasks dict)
        self._lock = threading.Lock()
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
        try:
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody= json.dumps(self._task_to_sqs_message(task)),
                MessageAttributes={
                    'priority': {
                    'StringValue': str(priority),
                    'DataType': 'Number'
                    }
                }
            )
            # Store task in local dict for lookup
            with self._lock:
                self._tasks[task.id] = task
            logger.info(f"Enqueued task {task_id} to SQS queue{response['MessageId']}.")
        except botocore.exceptions.ClientError as error:
            logger.error(f"Failed to enqueue task {task_id}: {error}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while enqueuing task {task_id}: {e}")
            raise
        return task_id           
    
    def dequeue(self) -> Optional[Task]:
        """Receive and return highest priority task from SQS."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=10  # Long polling
            )
            messages = response.get('Messages', [])
            if not messages:
                return None  # No messages available
            message = messages[0]
            task = self._sqs_message_to_task(message)
            # update task status to PROCESSING
            task.status = TaskStatus.PROCESSING
            task.updated_at = datetime.now()
            # Store receipt handle for later acknowledgment
            # task.receipt_handle = message['ReceiptHandle']
            with self._lock:
                self._tasks[task.id] = task
            logger.info(f"Dequeued task {task.id} from SQS.")
        except botocore.exceptions.ClientError as error:
            logger.error(f"Failed to dequeue task: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error while dequeuing task: {e}")
            return None
        return task
    
    def acknowledge_task(self, task_id: str, receipt_handle: str):
        """Acknowledge task completion and remove from SQS."""
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
            with self._lock:
                if task_id in self._tasks:
                    del self._tasks[task_id]
            logger.info(f"Acknowledged and deleted task {task_id} from SQS.")
        except botocore.exceptions.ClientError as error:
            logger.error(f"Failed to acknowledge task {task_id}: {error}")
        except Exception as e:
            logger.error(f"Unexpected error while acknowledging task {task_id}: {e}")

    def size(self) -> int:
        """Get approximate number of messages in queue."""
        try:
            response = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            size = int(response['Attributes']['ApproximateNumberOfMessages'])
            return size
        except botocore.exceptions.ClientError as error:
            logger.error(f"Failed to get queue size: {error}")
            return -1
        except Exception as e:
            logger.error(f"Unexpected error while getting queue size: {e}")
            return -1        
    
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
    def _sqs_message_to_task(self, message: MessageTypeDef) -> Task:
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
    