# api_server.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import uvicorn
import logging
from task_queue import TaskStatus, Task
from file_storage import FileStorage
from task_worker import TaskWorker, simple_task_processor
from contextlib import asynccontextmanager
from queue_factory import get_task_queue

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic models for request/response
class CreateTaskRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    priority: int = Field(..., ge=1, le=5)
    payload: Dict[str, Any] = Field(default_factory=dict)
    
class TaskResponse(BaseModel):
    id: str
    name: str
    priority: int
    payload: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime
    
class TaskStatusUpdate(BaseModel):
    status: str = Field(..., pattern="^(pending|processing|completed|failed)$")
    
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    queue_size: int
    worker_running: bool
# Initialize components (will be replaced with SQS in Task 2)
# queue = TaskQueue()
# storage = FileStorage()
# worker = TaskWorker(queue, storage, simple_task_processor)
# Use queue factory to get appropriate queue implementation
queue = get_task_queue() # Will use SQS if USE_SQS=true in environment
storage = FileStorage()
worker = TaskWorker(queue, storage, simple_task_processor)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting TaskFlow API...")
    worker.start()
    logger.info("TaskFlow API started successfully")
    yield
    logger.info("Shutting down TaskFlow API...")
    worker.stop()
    logger.info("TaskFlow API shutdown complete")

# FastAPI app
app = FastAPI(
    title="TaskFlow API",
    description="A distributed task processing system",
    version="2.0.0",
    lifespan=lifespan
)

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        queue_size=queue.size(),
        worker_running=worker.running
    )
    
@app.post("/tasks", response_model=TaskResponse, status_code=201)
async def create_task(task_request: CreateTaskRequest):
    """Create a new task."""
    try:
        task_id = queue.enqueue(
            name=task_request.name,
            priority=task_request.priority,
            payload=task_request.payload
        )
        task = queue.get_task(task_id)
        if not task:
            raise HTTPException(status_code=500, detail="Failed to create task")
        return task_to_response(task)
    except Exception as e:
        logger.error(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: str):
    """Get a specific task by ID."""
    task = queue.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task_to_response(task)

@app.put("/tasks/{task_id}/status")
async def update_task_status(task_id: str, status_update: TaskStatusUpdate):
    """Update task status."""
    if not validate_task_status(status_update.status):
        raise HTTPException(status_code=400, detail="Invalid status")
    success = queue.update_task_status(task_id, TaskStatus(status_update.status))
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"message": "Task status updated successfully"}

@app.get("/tasks", response_model=List[TaskResponse])
async def list_tasks(status: Optional[str] = None, limit: int = 100):
    """List tasks, optionally filtered by status."""
    tasks : List[Task] = []
    if status:
        if not validate_task_status(status):
            raise HTTPException(status_code=400, detail="Invalid status")
        tasks = queue.get_tasks_by_status(TaskStatus(status))
    else:
        # Get all tasks (you'll need to implement this in TaskQueue)
        tasks = queue.get_all_tasks()
    # Apply limit
    tasks = tasks[:limit]
    return [task_to_response(task) for task in tasks]

@app.get("/stats")
async def get_stats():
    """Get system statistics."""
    stats : Dict[str,Any] = {
        "queue_size": queue.size(),
        "worker_running": worker.running,
        "task_counts": {
            "pending": len(queue.get_tasks_by_status(TaskStatus.PENDING)),
            "processing":
            len(queue.get_tasks_by_status(TaskStatus.PROCESSING)),
            "completed": len(queue.get_tasks_by_status(TaskStatus.COMPLETED)),
            "failed": len(queue.get_tasks_by_status(TaskStatus.FAILED))
        }
    }
    return stats
@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task."""
    task = queue.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    # Delete from queue
    queue.delete_task(task_id)
    # Delete from storage
    storage.delete_task(task_id)
    return {"message": "Task deleted successfully"}
# Utility functions
def task_to_response(task: Task) -> TaskResponse:
    """Convert Task object to TaskResponse."""
    return TaskResponse(
    id=task.id,
    name=task.name,
    priority=task.priority,
    payload=task.payload,
    status=task.status.value,
    created_at=task.created_at,
    updated_at=task.updated_at
)
def validate_task_status(status: str) -> bool:
    """Validate task status string."""
    try:
        TaskStatus(status)
        return True
    except ValueError:
        return False
    
if __name__ == "__main__":
    uvicorn.run(
    "api_server:app",
    host="0.0.0.0",
    port=8000,
    reload=True,
    log_level="info"
    )