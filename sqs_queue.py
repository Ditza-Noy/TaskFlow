# sqs_queue.py
import json
import uuid
import botocore.exceptions
from datetime import datetime
from typing import List, Dict, Any
import logging
from task_queue import Task, TaskStatus, TaskQueue as BaseTaskQueue
from aws_config import AWSConfig
from base_task_queue import BaseTaskQueue
from mypy_boto3_sqs import SQSClient
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
    
    def dequeue(self, timeout: int = 10) -> Task | None:
        """Receive and return highest priority task from SQS."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                WaitTimeSeconds=timeout  # Long polling
            )
            messages = response.get('Messages', [])
            if not messages:
                return None  # No messages available
            message = messages[0]
            if not message:
                return None
            if 'Body' not in message:
                return None
            messageBody = message['Body']
            if not messageBody:
                return None
            task = self._sqs_message_to_task(messageBody)
            # update task status to PROCESSING
            task.status = TaskStatus.PROCESSING
            task.updated_at = datetime.now()
            # Store receipt handle for later acknowledgment
            task.receipt_handle = message.get('ReceiptHandle', '')
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
    
    def get_task(self, task_id: str) -> Task | None:
        """Get task by ID."""
        with self._lock:
            return self._tasks.get(task_id)
    
    def update_task_status(self, task_id: str, status: TaskStatus) -> bool:
        """Update task status."""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            task.status = status
            task.updated_at = datetime.now()
            self._tasks[task_id] = task
        return True
    
    def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        """Get all tasks with the specified status."""
        with self._lock:
            return [task for task in self._tasks.values() if task.status == status] 
        
    def get_all_tasks(self) -> List[Task]:
        """Get all tasks in the queue."""
        with self._lock:
            return list(self._tasks.values())
        
    def delete_task(self, task_id: str) -> bool:
        """Delete a task from SQS and local storage."""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            receipt_handle = task.receipt_handle
        if not receipt_handle:
            logger.error(f"No receipt handle for task {task_id}, cannot delete from SQS.")
            return False
        self.acknowledge_task(task_id, receipt_handle)
        return True
    
    def acknowledge_task(self, task_id: str, receipt_handle: str)-> bool:
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
            return False
        except Exception as e:
            logger.error(f"Unexpected error while acknowledging task {task_id}: {e}")
            return False
        return True

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
    def _sqs_message_to_task(self, message: str) -> Task:
        """Convert SQS message back to Task object."""
        body = json.loads(message)
        return Task(
            id=body['id'],
            name=body['name'],
            priority=body['priority'],
            payload=body['payload'],
            status=TaskStatus(body['status']),
            created_at=datetime.fromisoformat(body['created_at']),
            updated_at=datetime.fromisoformat(body['updated_at'])
        )
    