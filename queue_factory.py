# queue_factory.py
import os
from typing import Union
from task_queue import TaskQueue
from sqs_queue import SQSTaskQueue

def get_task_queue(use_sqs: bool | None = None) -> Union[TaskQueue, SQSTaskQueue]:
    """Factory function to create or get appropriate queue implementation."""
    if use_sqs == None:
        use_sqs = os.getenv("USE_SQS_QUEUE", "false").lower() == "true"
    if use_sqs:
        print("Using Amazon SQS for task queue")
        return SQSTaskQueue()
    else:
        print("Using local in-memory task queue")
        return TaskQueue()
    