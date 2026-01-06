# cached_task_operations.py
from typing import Any
from task_cache import TaskCache
from rds_storage import RDSTaskStorage
from task_queue import Task, TaskStatus
import logging
import time

logger = logging.getLogger(__name__)

class CachedTaskOperations:
    """Task operations with caching layer."""
    def __init__(self):
        self.rds_storage = RDSTaskStorage()

    def get_task_metadata(self, task_id: str) -> dict[str, Any] | None:
        """Get task metadata with cache-aside pattern."""
        cache_key = f"task_metadata:{task_id}"
        task = TaskCache().get(cache_key)
        if task:
            return task
        else:
            start_time = time.time()
            task_data = self.rds_storage.load_task_metadata(task_id)
            db_time = (time.time() - start_time) * 1000  # in milliseconds
            logger.info(f"DB read time for task_id {task_id}: {db_time:.2f} ms")
            if task_data:
                TaskCache().set(cache_key, task_data, ttl_seconds=300)
            return task_data
    
    def update_task_status(self, task_id: str, status: TaskStatus, error_message: str | None = None) -> bool:
        """Update task status and invalidate cache."""
        # TODO: Implement cache-aside pattern for writing
        # 1. Update in database FIRST: self.rds_storage.update_task_status(...)
        # 2. If update successful:
        # a. Delete specific cache entry: f"task_metadata:{task_id}"
        # b. Invalidate related queries: pattern "tasks_by_status:"
        # c. Log the cache invalidation
        # 3. Return success status
        pass