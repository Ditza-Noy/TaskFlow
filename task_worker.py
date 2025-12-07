import threading
import time
import logging
from typing import Callable
from task_queue import TaskQueue, Task, TaskStatus
from sqs_queue import SQSTaskQueue
from file_storage import FileStorage
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(threadName)s - %(levelname)s -%(message)s')
logger = logging.getLogger(__name__)
class TaskWorker:
    
    def __init__(self, queue: TaskQueue | SQSTaskQueue, storage: FileStorage,task_processor: Callable[[Task], bool]):
        self.queue = queue
        self.storage = storage
        self.task_processor = task_processor
        self.running = False
        self.worker_thread = None
        self.DEQUEUE_TIMEOUT = 1  # seconds

    def start(self):
        """Start the worker thread."""
        #Start worker thread if not already running
        if not self.running:
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, name="TaskWorkerThread")
            self.worker_thread.start()

    def stop(self):
        """Stop the worker thread gracefully."""
        # Signal stop and wait for thread to finish
        self.running = False
        # wait for thread to finish
        if self.worker_thread:
            self.worker_thread.join()
            logger.info("Worker thread has been stopped")
            self.worker_thread = None

    def _worker_loop(self):
        """Main worker loop - runs in separate thread."""
        logger.info("Worker thread started")
        while self.running:
            try:
                # Get task from queue
                task = self.queue.dequeue(self.DEQUEUE_TIMEOUT)
                if task is None:
                    continue  # Timeout expired, loop again
                # Process the task using task_processor
                # logger.info(f"Processing task: {task.name} (ID: {task.id})")
                success = self.task_processor(task)
                # Update task status based on result (COMPLETED/FAILED)
                final_status = TaskStatus.COMPLETED if success else TaskStatus.FAILED
                task.status = final_status
                if success:
                    logger.info(f"Task {task.id} completed successfully")
                else:
                    logger.error(f"Task {task.id} failed during processing")
                task.updated_at = datetime.now()
                # Save task to storage
                self.storage.save_task(task)
                pass
            except Exception as e:
                logger.error(f"Error processing task: {e}")

        logger.info("Worker thread stopped")

        # Example task processor function
def simple_task_processor(task: Task) -> bool:
        """
        Simple task processor that simulates work.
        Returns True for success, False for failure.
        """
        logger.info(f"Processing task: {task.name} (ID: {task.id})")

        # Simulate processing time
        time.sleep(0.1)


        # Simulate occasional failures (10% failure rate)
        import random
        if random.random() < 0.1:
            logger.error(f"Task {task.id} failed during processing")
            return False

        logger.info(f"Task {task.id} completed successfully")
        return True