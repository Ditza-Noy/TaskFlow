import json
from base_file_storage import BaseFileStorage
from task_queue import Task
import logging
from pydantic import ValidationError
from typing import Any
from s3_config import S3Config
from botocore.exceptions import ClientError
from datetime import datetime, timezone




logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class S3Storage(BaseFileStorage):
    def __init__(self):
        self.s3_config = S3Config()
        self.s3_client = self.s3_config.s3_client
        self.bucket_name = self.s3_config.bucket_name
        # Initialize bucket
        try:
            self.s3_config.create_bucket_if_not_exists()
        except Exception as e:
            logger.error(f"Failed to initialize bucket: {e}")
            raise
        self.s3_config.setup_bucket_lifecycle()
        logger.info(f"S3 storage initialized with bucket: {self.bucket_name}")

    def _get_task_s3_key(self, task_id: str) -> str:
        """
        Generate S3 key for task based on ID (maintains directory structure).
        """
        if not task_id:
            raise ValueError("Task ID cannot be empty")
        return f"tasks/{task_id[:min(2,len(task_id))]}/{task_id}.json" 
    
    def save_task(self, task: Task) -> bool:
        """
        Save task to S3. 
        
        TODO: Convert task to JSON. 
        TODO: Upload to S3 with proper content type. 
        """
        task_s3_key = self._get_task_s3_key(task.id)
        task_data = task.model_dump(mode='json')
        # Upload to S3
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=task_s3_key,
            Body=json.dumps(task_data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Task {task.id} saved to S3: s3://{self.bucket_name}/{task_s3_key}")
        return True

    def load_task(self, task_id: str) -> Task | None:
        """Load task from S3."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=self._get_task_s3_key(task_id)
            )
            task_data = json.loads(response['Body'].read())
            return Task(**task_data)
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"Task {task_id} not found in S3.")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON for task {task_id}: {e}")
            return None
        except ValidationError as e:
            logger.error(f"Failed to parse task {task_id}: {e}")
            return None
        except ClientError as e:
            logger.error(f"S3 error loading task {task_id}: {e}")
            return None

    def delete_task(self, task_id: str) -> bool:
        """
        Delete task from S3.
        Returns True if task existed and was deleted, False if task didn't exist or deletion failed.
        """
        try:
            # Check if object exists first
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=self._get_task_s3_key(task_id)
            )
            
            # Object exists, proceed with deletion
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=self._get_task_s3_key(task_id)
            )
            logger.info(f"Task {task_id} deleted from S3.")
            return True
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"Task {task_id} not found in S3, nothing to delete.")
            return False
        except ClientError as e:
            logger.error(f"Failed to delete task {task_id} from S3: {e}")
            return False

    def list_all_tasks(self) -> list[str]:
        """List all task IDs in S3."""
        try:
            task_ids: list[str] = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix='tasks/'):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Extract task ID from S3 key
                        # tasks/ab/abc123.json -> abc123
                        # check id key exists
                        if 'Key' in obj:
                            key = obj['Key']
                            if key.endswith('.json'):
                                task_id = key.split('/')[-1].replace('.json', '')
                                task_ids.append(task_id)
            return task_ids
        except ClientError as e:
            logger.error(f"Error listing tasks from S3: {e}")
            return []

    def backup_all_tasks(self) -> str:
        """Create backup by copying to backup prefix."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_prefix = f"backups/backup_{timestamp}/"
        try:
            # TODO: List all current tasks
            task_ids = self.list_all_tasks()
            # TODO: Copy each task to backup prefix
            for task_id in task_ids:
                source_key = self._get_task_s3_key(task_id)
                backup_key = f"{backup_prefix}{source_key}"

                self.s3_client.copy_object(
                    Bucket=self.bucket_name,
                    CopySource={'Bucket': self.bucket_name, 'Key': source_key},
                    Key=backup_key
                )
            # TODO: Create backup manifest file
            manifest: dict[str, Any] = {
                'backup_id': timestamp,
                'created_at': datetime.now(timezone.utc).isoformat(),
                'task_count': len(task_ids),
                'task_ids': task_ids
            }
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{backup_prefix}manifest.json",
                Body=json.dumps(manifest, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Backup completed: {backup_prefix} ({len(task_ids)} tasks)")
            return timestamp
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            return ""

    def get_storage_stats(self) -> dict[str, Any]:
        """Get S3 storage statistics."""
        try:
            # TODO: Get bucket statistics
            paginator = self.s3_client.get_paginator('list_objects_v2')
            stats = {
                'total_objects': 0,
                'total_size_bytes': 0,
                'tasks_count': 0,
                'backups_count': 0
            }
            for page in paginator.paginate(Bucket=self.bucket_name):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        stats['total_objects'] += 1
                        if 'Size' in obj:
                            stats['total_size_bytes'] += obj['Size']
                        if 'Key' in obj:
                            if obj['Key'].startswith('tasks/'):
                                stats['tasks_count'] += 1
                            elif obj['Key'].startswith('backups/'):
                                stats['backups_count'] += 1
            return stats
        except Exception as e:
            logger.error(f"Error getting storage stats: {e}")
            return {}
