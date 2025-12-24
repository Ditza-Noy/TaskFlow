import pytest
import os
import json
from datetime import datetime
from s3_storage import S3Storage
from task_queue import Task  # If TaskStatus is exportable, you could import it here too
from botocore.exceptions import ClientError

# --- IMPORTANT: Ensure you are using a test bucket ---
os.environ["S3_BUCKET_NAME"] = "taskflow-bucket-2025-12-24-techgymbooster-test"

class TestS3Integration:
    """
    REAL Integration tests. 
    WARNING: This will create actual resources in your AWS account.
    """

    @pytest.fixture(scope="class", autouse=True)
    def setup_storage(self):
        """Initializes storage once for the test class."""
        storage = S3Storage()
        yield storage
        
        # Cleanup
        print("\nCleaning up integration test bucket...")
        try:
            paginator = storage.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=storage.bucket_name):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        storage.s3_client.delete_object(Bucket=storage.bucket_name, Key=obj['Key'])
            
            storage.s3_client.delete_bucket(Bucket=storage.bucket_name)
            print(f"Deleted bucket: {storage.bucket_name}")
        except ClientError as e:
            print(f"Cleanup failed: {e}")

    def test_connection_and_bucket_creation(self, setup_storage):
        """Verifies the bucket was actually created in AWS."""
        storage = setup_storage
        response = storage.s3_client.head_bucket(Bucket=storage.bucket_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    def test_full_task_lifecycle(self, setup_storage):
        """Tests Save -> Load -> List -> Delete on real AWS infrastructure."""
        storage = setup_storage
        
        # 1. Create a real Task object
        task_id = f"int_test_{int(datetime.now().timestamp())}"
        
        test_task = Task(
            id=task_id, 
            name="Integration Test", 
            status="pending",       
            priority=1,
            payload={"test": 123},
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        # 2. Save to S3
        save_result = storage.save_task(test_task)
        assert save_result is True

        # 3. Load from S3
        loaded_task = storage.load_task(task_id)
        assert loaded_task is not None
        assert loaded_task.id == task_id
        assert loaded_task.name == "Integration Test"
        
        # FIX: Compare the Enum's *value* to the string, or import TaskStatus and compare objects
        assert loaded_task.status.value == "pending"

        # 4. List tasks
        all_ids = storage.list_all_tasks()
        assert task_id in all_ids

        # 5. Delete task
        delete_result = storage.delete_task(task_id)
        assert delete_result is True

        # 6. Confirm deletion
        final_check = storage.load_task(task_id)
        assert final_check is None

    def test_backup_logic(self, setup_storage):
        """Tests copying objects within S3 and manifest creation."""
        storage = setup_storage
        task_id = "backup_test_task"

        storage.save_task(Task(
            id=task_id, 
            name="Backup Test",
            status="pending",
            priority=1,
            payload={},
            created_at=datetime.now(),
            updated_at=datetime.now()
        ))

        # Run backup
        timestamp = storage.backup_all_tasks()
        assert timestamp != ""

        # Verify manifest exists in S3
        manifest_key = f"backups/backup_{timestamp}/manifest.json"
        response = storage.s3_client.get_object(Bucket=storage.bucket_name, Key=manifest_key)
        manifest = json.loads(response['Body'].read())
        
        assert manifest['backup_id'] == timestamp
        assert task_id in manifest['task_ids']

    def test_storage_stats(self, setup_storage):
        """Verifies stats correctly count real objects."""
        storage = setup_storage
        stats = storage.get_storage_stats()
        
        assert 'total_objects' in stats
        assert stats['total_objects'] > 0
        assert stats['tasks_count'] >= 1