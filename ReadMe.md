# TaskFlow - Priority Task Processing System

A thread-safe task processing system built with Python, featuring priority queuing, persistent storage, and asynchronous task execution.

## System Architecture

The system consists of three core components that work together to manage task lifecycle:

### 1. TaskQueue (task_queue.py)

The priority queue implementation using a **hybrid data structure approach** for optimal performance.

#### Key Design Decisions:

**Min-Heap for Priority Queue**
- **Method**: `heapq` (Python's built-in min-heap)
- **Why**: Provides O(log n) time complexity for enqueue and dequeue operations
- **Implementation**: Tasks with lower priority numbers are processed first (1 = highest priority)

**Dictionary for Fast Lookups**
- **Method**: `_tasks` dictionary mapping task IDs to Task objects
- **Why**: Enables O(1) time complexity for status lookups and updates by task ID
- **Trade-off**: Uses additional memory for faster access patterns

**Thread Synchronization**
- **Method**: `threading.Lock` and `threading.Condition` for thread-safe operations
- **Why**:
  - Prevents race conditions when multiple threads access shared data
  - Condition variable allows worker threads to wait efficiently when queue is empty
  - Workers are notified immediately when new tasks arrive

**Pydantic for Data Validation**
- **Method**: `BaseModel` from Pydantic for Task model definition
- **Why**:
  - Automatic data validation and type checking
  - Built-in JSON serialization support
  - Clear schema definition

### 2. FileStorage (file_storage.py)

Handles persistent storage of tasks with focus on reliability and thread safety.

#### Key Design Decisions:

**Atomic File Operations**
- **Method**: `tempfile.mkstemp()` followed by `os.replace()`
- **Why**:
  - Ensures data integrity during writes
  - Prevents partial file writes if system crashes
  - Replace operation is atomic on most file systems

**Directory Sharding**
- **Method**: Tasks stored in subdirectories based on first 2 characters of task ID
- **Why**:
  - Prevents file system performance degradation with many files in single directory
  - Most file systems slow down with >10,000 files in one directory

**Thread-Safe File Access**
- **Method**: `threading.Lock` protects all file operations
- **Why**: Prevents concurrent writes to same file and race conditions

**Backup and Restore**
- **Method**: JSON backup files with timestamp naming
- **Why**:
  - Enables disaster recovery
  - Simple format for manual inspection
  - Timestamped backups allow point-in-time recovery

**Pydantic Integration**
- **Method**: `model_dump(mode='json')` and `model_validate()`
- **Why**:
  - Automatic handling of datetime and enum serialization
  - Type validation on load prevents corrupted data
  - Eliminates manual serialization code

### 3. TaskWorker (task_worker.py)

Background worker that processes tasks asynchronously.

#### Key Design Decisions:

**Separate Worker Thread**
- **Method**: Dedicated background thread running `_worker_loop()`
- **Why**:
  - Allows main program to continue while tasks process
  - Clean separation between task submission and execution
  - Worker can be started/stopped independently

**Graceful Shutdown**
- **Method**: `running` flag with thread join
- **Why**:
  - Ensures in-flight tasks complete before shutdown
  - Prevents data loss or corrupted state
  - Clean resource cleanup

**Timeout-Based Dequeuing**
- **Method**: `dequeue(timeout=1.0)` in worker loop
- **Why**:
  - Worker can check `running` flag periodically
  - Prevents infinite blocking on empty queue
  - Enables responsive shutdown (stops within 1 second)


**Automatic Status Updates**
- **Method**: Worker updates task status based on processor return value
- **Why**:
  - Centralizes status management logic
  - Ensures consistent status tracking
  - Automatically persists final status to storage

## How Components Work Together

1. **Task Creation**: `TaskQueue.enqueue()` creates a task with unique ID and adds to both heap and dictionary
2. **Task Processing**: `TaskWorker` thread continuously dequeues highest priority pending task
3. **Execution**: Worker calls the processor function and updates task status
4. **Persistence**: Worker saves completed/failed tasks to `FileStorage`
5. **Recovery**: System can restore tasks from backup files



# TaskFlow - Distributed Task Processing API (Week 2)

A distributed task processing system transformed from a local script into a RESTful API service. This version introduces Amazon SQS for reliable message queuing and FastAPI for HTTP access, allowing the system to scale beyond a single machine.

## System Architecture

The system now operates as a service with the following enhanced components:

### 1. REST API Layer (`api_server.py`)
Exposes TaskFlow functionality via HTTP endpoints using **FastAPI**.
* **Framework**: FastAPI with Uvicorn for high-performance async execution.
* **Endpoints**:
    * `POST /tasks`: Enqueue new tasks.
    * `GET /tasks/{task_id}`: Retrieve task status and payload.
    * `GET /stats`: Monitor queue size and worker status.
* **Middleware**: Custom logging middleware to track request duration and unique request IDs.

### 2. Queue Abstraction & SQS (`sqs_queue.py`)
Replaces the local heap with **Amazon SQS** while maintaining the original interface via the Adapter Pattern.
* **Factory Pattern**: `queue_factory.py` selects between `TaskQueue` (Local) and `SQSTaskQueue` (AWS) based on environment variables.
* **SQS Features Used**:
    * **Long Polling**: Reduces empty responses and cost.
    * **Visibility Timeout**: Ensures tasks are not lost if a worker crashes.
    * **Message Attributes**: Stores priority metadata alongside the payload.

### 3. Structured Logging (`logging_config.py`)
Production-ready logging system replacing standard print statements.
* **Format**: JSON logs for easy parsing by monitoring tools (e.g., CloudWatch, Datadog).
* **Context**: Automatically includes `request_id`, `task_id`, and `duration_ms` in log entries.

---

### 2. Updated README for Week 3 (Scaling & Cloud Storage)

This version adds Amazon S3, RDS, Load Balancing, and Infrastructure as Code.

```markdown
# TaskFlow - Scalable Cloud-Native Task System (Week 3)

An enterprise-grade evolution of TaskFlow designed for horizontal scaling. This version migrates storage to Amazon S3 (Object Storage) and RDS (Relational Database), introduces a Load Balancer, and manages infrastructure using CloudFormation.

## Architecture Evolution

The system has moved from a monolithic script to a fully decoupled microservices-ready architecture:

### 1. Cloud Storage Layer
* **Object Storage (S3)**: Replaces local file storage for task payloads and artifacts.
    * **Lifecycle Policies**: Automatically moves old data to Glacier after 90 days to reduce costs.
    * **Design**: Uses `s3_storage.py` which implements the standard storage interface.
* **Metadata Storage (RDS PostgreSQL)**: Replaces in-memory dictionaries for task tracking.
    * **Connection Pooling**: Uses `psycopg2` pool to handle high concurrency efficiently.
    * **Schema**: Relational model with indexing on `status` and `priority` for fast querying.

### 2. Horizontal Scaling
* **Load Balancer (`load_balancer.py`)**: A custom round-robin load balancer that sits in front of multiple API instances.
    * **Health Checks**: Periodically pings `/health` on backend instances to remove unhealthy nodes.
* **Stateless API**: The API servers are now stateless, allowing us to run multiple instances (e.g., ports 8001, 8002, 8003) connected to the same shared SQS and RDS backends.

### 3. Infrastructure as Code (IaC)
* **CloudFormation**: All AWS resources (S3 buckets, SQS queues, RDS instances, VPCs) are defined in `cloudformation/taskflow-infrastructure.yaml`.
* **Deploy Script**: `deploy.py` automates the stack creation and updates `.env` with the new resource outputs.


