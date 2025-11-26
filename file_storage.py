import json
import os
from pathlib import Path
from typing import Optional, List
from task_queue import Task
from threading import Lock
import logging
from datetime import datetime
from pydantic import ValidationError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class FileStorage:
    def __init__(self, base_path: str = "taskflow_data"):
        self.base_path = Path(base_path)
        self.tasks_dir = self.base_path / "tasks"
        self.backups_dir = self.base_path / "backups"
        self._lock = Lock()
        self._create_directories()

    def _create_directories(self):
        """Create necessary directory structure."""
        with self._lock:
            self.tasks_dir.mkdir(parents=True, exist_ok=True)
            self.backups_dir.mkdir(parents=True, exist_ok=True)

    def _get_task_file_path(self, task_id: str) -> Path:
        """Get the file path for a task based on its ID."""
        if not task_id:
            raise ValueError("Task ID cannot be empty")
        return Path(self.tasks_dir, task_id[:min(2,len(task_id))], f"{task_id}.json")


    def save_task(self, task: Task) -> bool:
        """Save a task to file storage."""
        # TODO: Convert task to dict and save as JSON
        # Handle file creation and error cases
        # json_data:Dict[str,Any] = self.task_to_dict(task)
        task_path = self._get_task_file_path(task.id)
        with self._lock:
            task_path.parent.mkdir(parents=True, exist_ok=True)
            with open(task_path, 'w') as f:
                json_data = task.model_dump_json()
                f.write(json_data)
                return True
        return False

    def load_task(self, task_id: str) -> Optional[Task]:
        """Load a task from file storage."""
        task_path = self._get_task_file_path(task_id)
        
        with self._lock:
            try:
                with open(task_path, 'r') as f:
                    json_data = json.load(f)
                    task: Task = Task.model_validate(json_data)
                    return task
            except FileNotFoundError:
                return None
            except ValidationError as e:
                logger.error(f"Corrupted task file {task_id}: {e}")
                return None
            except Exception as e:
                logger.error(f"Error loading task {task_id}: {e}")
                return None

    def delete_task(self, task_id: str) -> bool:
        """Delete a task file."""
        task_path = self._get_task_file_path(task_id)
        with self._lock:
            try:
                task_path.unlink(missing_ok=True)
                return True
            except (PermissionError, OSError) as e:
                logger.error(f"Failed to delete task {task_id}: {e}")
                return False

    def list_all_tasks(self) -> List[str]:
        """List all task IDs in storage."""
        # TODO: Recursively find all .json files and extract task IDs
        task_ids: List[str] = []
        with self._lock:
            for _, _, files in os.walk(self.tasks_dir):
                for file in files:
                    if file.endswith('.json'):
                        task_id = file[:-5]  # Remove .json extension
                        task_ids.append(task_id)
        return task_ids
    
    def backup_all_tasks(self) -> str:
        """Create a backup of all tasks. Returns backup
        filename."""
        # TODO: Create timestamped backup file with all tasks
        backup_data: List[str] = []
        with self._lock:
            tasks = self.list_all_tasks()
            for task_id in tasks:
                task = self.load_task(task_id)
                if task:
                    backup_data.append(task.model_dump_json())
            backup_filename = self.backups_dir / f"backup_{int(datetime.now().timestamp())}.json"
            try:
                backup_filename.parent.mkdir(parents=True, exist_ok=True)
                with open(backup_filename, 'w') as f:
                    json.dump(backup_data, f)
            except Exception as e:
                logger.error(f"Failed to create backup: {e}")
                return ""
        return str(backup_filename)

    def restore_from_backup(self, backup_filename: str) -> int:
        """Restore tasks from backup. Returns number of tasks
        restored."""
        # read backup file
        with self._lock:
            try:
                with open(backup_filename, 'r') as f:
                    backup_data: List[str] = json.load(f)
            except FileNotFoundError:
                logger.error(f"Backup file {backup_filename} not found.")
                return 0
            except json.JSONDecodeError as e:
                logger.error(f"Corrupted backup file {backup_filename}: {e}")
                return 0
        # restore tasks
        restored_count = 0
        for task_json in backup_data:
            try:
                task_data = json.loads(task_json)
                task: Task = Task.model_validate(task_data)
                if self.save_task(task):
                    restored_count += 1
            except ValidationError as e:
                logger.error(f"Failed to restore task from backup: {e}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid task data in backup: {e}")
        return restored_count
                    

    # # Helper function to convert Task to/from dict
    # def task_to_dict(self, task: Task) -> Dict[str, Any]:
    #     """Convert Task object to dictionary for JSON
    #     serialization."""
    #     # TODO: Handle datetime and enum serialization
    #     pass
    # def dict_to_task(self, data: Dict[str,Any]) -> Optional[Task]:
    #     """Convert dictionary back to Task object."""
    #     # TODO: Handle datetime parsing and enum conversion
    #     pass