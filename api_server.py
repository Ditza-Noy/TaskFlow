# api_server.py
import sys
import uuid
import time
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field
from typing import Any
from datetime import datetime, timezone
import uvicorn
import logging
from task_queue import TaskStatus, Task
from file_storage import FileStorage
from task_worker import TaskWorker, simple_task_processor
from contextlib import asynccontextmanager
from queue_factory import get_task_queue
from logging_config import setup_logging
from collections.abc import Callable, Awaitable # Native imports (no 'typing')

# Define a clean alias using Python 3.12 syntax
type CallNext = Callable[[Request], Awaitable[Response]]

# Setup logging
setup_logging(use_json=True)
logger = logging.getLogger(__name__)



# Pydantic models for request/response
class CreateTaskRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    priority: int = Field(..., ge=1, le=5)
    payload: dict[str, Any] = Field(default_factory=dict)
    
class TaskResponse(BaseModel):
    id: str
    name: str
    priority: int
    payload: dict[str, Any]
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

@app.middleware("http")
async def logging_middleware(request: Request, call_next: CallNext ) -> Response:
    """Middleware to log all API requests."""
    request_id = str(uuid.uuid4())
    start_time = time.time()
    # Add request ID to request state
    request.state.request_id = request_id
    # Log incoming request
    logger.info(
    "Incoming request",
        extra={
            'request_id': request_id,
            'method': request.method,
            'url': str(request.url),
            'client_ip': request.client.host if request.client else None,
            'user_agent': request.headers.get("user-agent"),
            'event': 'request_received'
        }
    )
    # Process request
    response = await call_next(request)
    # Calculate duration
    duration = (time.time() - start_time) * 1000
    # Log response
    logger.info("Request completed",
        extra={
            'request_id': request_id,
            'status_code': response.status_code,
            'duration_ms': round(duration, 2),
            'event': 'request_completed'
        }
    )
    return response

@app.get("/health", response_model=HealthResponse)
def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        queue_size=queue.size(),
        worker_running=worker.running
    )
    
@app.post("/tasks", response_model=TaskResponse, status_code=201)
def create_task(task_request: CreateTaskRequest):
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
def get_task(task_id: str):
    """Get a specific task by ID."""
    task = queue.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task_to_response(task)

@app.put("/tasks/{task_id}/status")
def update_task_status(task_id: str, status_update: TaskStatusUpdate):
    """Update task status."""
    if not validate_task_status(status_update.status):
        raise HTTPException(status_code=400, detail="Invalid status")
    success = queue.update_task_status(task_id, TaskStatus(status_update.status))
    if not success:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"message": "Task status updated successfully"}

@app.get("/tasks", response_model=list[TaskResponse])
def list_tasks(status: str | None = None, limit: int = 100):
    """List tasks, optionally filtered by status."""
    tasks : list[Task] = []
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
def get_stats():
    """Get system statistics."""
    stats : dict[str,Any] = {
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
def delete_task(task_id: str):
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
    
def start_server(host: str = "0.0.0.0", port: int = 8000):
    """
    Function to start the Uvicorn server programmatically.
    """
    uvicorn.run(
        "api_server:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )
    
if __name__ == "__main__":
    # Default port
    port = 8000
    
    # Check command line args (e.g., python api_server.py 8001)
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print(f"Invalid port: {sys.argv[1]}")
            sys.exit(1)
            
    # Call the function
    start_server(port=port)