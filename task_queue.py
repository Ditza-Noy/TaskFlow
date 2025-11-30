from enum import Enum
import heapq
import threading
import uuid
from datetime import datetime
from typing import Optional, List, Any, Dict
from pydantic import BaseModel


class TaskStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    
class Task(BaseModel):
    id: str
    name: str
    priority: int # 1 = highest, 5 = lowest
    payload: Dict[str, Any]
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    def __lt__(self, other: "Task") -> bool:
        # For heapq comparison - lower priority number = higher priority
        return self.priority < other.priority

class TaskQueue:
    def __init__(self):
        # The priority queue (min-heap)
        self._queue: List[Task] = []
        # Task dictionary for O(1) status lookups and updates
        self._tasks: Dict[str, Task] = {}
        # Lock for protecting shared data (the queue and tasks dict)
        self._lock = threading.Lock()
        # Condition variable for thread synchronization: 
        # allows dequeuing threads to wait when the queue is empty, 
        # and enqueuing threads to notify waiting threads when a new task arrives.
        self._condition = threading.Condition(self._lock)

    def enqueue(self, name: str, priority: int, payload: Dict[str, Any]) -> str:
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
        
        # Acquire lock and notify waiting threads
        with self._condition:
            heapq.heappush(self._queue, task)
            self._tasks[task.id] = task
            # Notify one waiting thread that a task is available
            self._condition.notify() 
        return task.id

    def dequeue(self, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Remove and return the highest priority task.
        Blocks until a task is available or the timeout expires.
        """
        with self._condition:
            # Wait while the queue is empty.
            # If the queue is empty, self._condition.wait() releases the lock 
            # and blocks until another thread calls notify() or notify_all().
            # The 'while' loop prevents 'spurious wakeups' (waking up when no task is actually there).
            while not self._queue:
                if timeout is not None and timeout <= 0:
                    return None
                
                # Wait returns True if notified, False if timeout expires
                if not self._condition.wait(timeout=timeout):
                    # Timeout expired and queue is still empty
                    return None

            # A task is guaranteed to be available here
            task = heapq.heappop(self._queue)
            
            # Update task status and record changes in the dictionary
            task.status = TaskStatus.PROCESSING
            task.updated_at = datetime.now()
            self._tasks[task.id] = task
            return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Retrieve a task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def update_task_status(self, task_id: str, status: TaskStatus)-> bool:
        """Update task status. Returns True if successful."""
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return False
            
            # Only allow updating tasks that exist
            if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED] and status not in [TaskStatus.PENDING]:
                 # Prevent re-processing completed/failed tasks unless explicitly marked PENDING
                 return False 
                 
            # Update state
            task.status = status
            task.updated_at = datetime.now()
            self._tasks[task_id] = task

            # If moving back to PENDING, re-add to the priority queue
            # This is useful for task retry mechanisms
            if status == TaskStatus.PENDING:
                heapq.heappush(self._queue, task)
                # Important: Notify a waiting worker thread if a task is re-queued
                self._condition.notify()
                
            return True

    def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        """Get all tasks with a specific status."""
        with self._lock:
            return [task for task in self._tasks.values() if task.status == status]

    def size(self) -> int:
        """Return the number of pending tasks."""
        with self._lock:
            return len(self._queue)