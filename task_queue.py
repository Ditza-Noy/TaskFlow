from dataclasses import dataclass
from enum import Enum
import heapq
import threading
import uuid
from datetime import datetime
from typing import Optional, List


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    
@dataclass
class Task:
    id: str
    name: str
    priority: int # 1 = highest, 5 = lowest
    payload: dict
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    def __lt__(self, other):
    # For heapq comparison - lower priority number = higher priority
        return self.priority < other.priority

class TaskQueue:
    def __init__(self):
        self._queue = []
        self._tasks = {} # id -> Task mapping for O(1) lookups
        self._lock = threading.Lock()
    def enqueue(self, name: str, priority: int, payload: dict) -> str:
        """Add a task to the queue. Returns task ID."""
        task = Task(
            id=str(uuid.uuid4()),
            name=name,
            priority=priority,
            payload=payload,
            status=TaskStatus.PENDING,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        with self._lock:
            heapq.heappush(self._queue, task)
            self._tasks[task.id] = task
        return task.id
    
    def dequeue(self) -> Optional[Task]:
        """Remove and return the highest priority task."""
        with self._lock:
            if not self._queue:
                return None
            task = heapq.heappop(self._queue)
            task.status = TaskStatus.PROCESSING
            task.updated_at = datetime.now()
            self._tasks[task.id] = task
            return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Retrieve a task by ID."""
        with self._lock:
            task = self._tasks.get(task_id)
            return task
        
    def update_task_status(self, task_id: str, status: TaskStatus)-> bool:
        """Update task status. Returns True if successful."""
        # TODO: Implement this method
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            task.status = status
            task.updated_at = datetime.now()
            self._tasks[task_id] = task
            
            if status == TaskStatus.PENDING:
                heapq.heappush(self._queue, task)
            
            return True

    def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        """Get all tasks with a specific status."""
        with self._lock:
            return [task for task in self._tasks.values() if task.status == status]

    def size(self) -> int:
        """Return the number of pending tasks."""
        with self._lock:
            return len(self._queue)