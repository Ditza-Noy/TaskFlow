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

