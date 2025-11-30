# main.py
import time
import random
from task_queue import TaskQueue,  TaskStatus
from file_storage import FileStorage
from task_worker import TaskWorker, simple_task_processor
from typing import  Any, Dict

def generate_sample_tasks(queue: TaskQueue, count: int = 20):
    """Generate sample tasks for testing."""
    task_names = [
    "Process user registration", "Send welcome email","Generate report","Backup database", \
    "Update user profile", "Send notification", "Process payment", "Generate invoice", \
    "Update inventory", "Send marketing email" ]

    for i in range(count):
        name = random.choice(task_names)
        priority = random.randint(1, 5)
        payload: Dict[str,Any] = {
            "user_id": f"user_{i}",
            "data": f"sample_data_{i}",
            "timestamp": time.time()
        }
        task_id = queue.enqueue(name, priority, payload)
        # print(f"Created task: {task_id} - {name} (Priority: {priority})")
        
def main():
    """Main demonstration function."""
    print("=== TaskFlow Foundation Demo ===\n")

    # Initialize components
    queue = TaskQueue()
    storage = FileStorage()
    worker = TaskWorker(queue, storage, simple_task_processor)

    # Generate sample tasks
    print("1. Generating sample tasks...")
    generate_sample_tasks(queue)
    print(f"Queue size: {queue.size()}\n")

    # Start worker
    print("2. Starting task worker...")
    worker.start()

    # Monitor progress
    print("3. Processing tasks...\n")
    while queue.size() > 0:
        pending = len(queue.get_tasks_by_status(TaskStatus.PENDING))
        processing = len(queue.get_tasks_by_status(TaskStatus.PROCESSING))
        completed = len(queue.get_tasks_by_status(TaskStatus.COMPLETED))
        failed = len(queue.get_tasks_by_status(TaskStatus.FAILED))
        print(f"Status - Pending: {pending}, Processing: {processing}, Completed: {completed}, Failed: {failed}")
        time.sleep(2)

    # Stop worker
    print("\n4. Stopping worker...")
    worker.stop()

    # Show final statistics
    all_tasks = storage.list_all_tasks()
    print(f"\n5. Final Results:")
    print(f"Total tasks processed: {len(all_tasks)}")

    completed_tasks = queue.get_tasks_by_status(TaskStatus.COMPLETED)
    failed_tasks = queue.get_tasks_by_status(TaskStatus.FAILED)
    print(f"Successful: {len(completed_tasks)}")
    print(f"Failed: {len(failed_tasks)}")

    # Create backup
    backup_file = storage.backup_all_tasks()
    print(f"Backup created: {backup_file}")
    
    # Restore from backup (for demonstration, we will just show the count)
    restored_count = storage.restore_from_backup(backup_file)
    print(f"Tasks restored from backup: {restored_count}")

    print("\n=== Demo Complete ===")
    
if __name__ == "__main__":
    main()