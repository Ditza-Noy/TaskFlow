# rds_storage.py
import json
import psycopg2
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
from contextlib import contextmanager
from task_queue import Task, TaskStatus
from rds_config import RDSConfig

logger = logging.getLogger(__name__)


class RDSTaskStorage:
    """RDS-backed task metadata storage."""

    def __init__(self):
        self.rds_config = RDSConfig()
        self.rds_config.create_connection_pool()
        self._initialize_schema()

    def _initialize_schema(self):
        """Initialize database schema."""
        try:
            # TODO: Read schema.sql and execute
            with open('schema.sql', 'r') as schema_file:
                schema_sql = schema_file.read()
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(schema_sql)

                conn.commit()
                logger.info("Database schema initialized")
        except Exception as e:
            logger.error(f"Error initializing schema: {e}")
            raise

    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = self.rds_config.get_connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                self.rds_config.return_connection(conn)

    def save_task_metadata(self, task: Task, s3_key: Optional[str] = None) -> bool:
        """Save task metadata to RDS."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # TODO: Insert or update task metadata
                    cursor.execute("""
                        INSERT INTO tasks (id, name, priority, status,
                        created_at, updated_at, payload_s3_key)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        priority = EXCLUDED.priority,
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at,
                        payload_s3_key = EXCLUDED.payload_s3_key
                    """, (
                        task.id,
                        task.name,

                        task.priority,
                        task.status.value,
                        task.created_at,
                        task.updated_at,
                        s3_key
                    ))
                conn.commit()
                logger.info(f"Task metadata {task.id} saved to RDS")
                return True
        except Exception as e:
            logger.error(f"Error saving task {task.id} to RDS: {e}")
            return False

    def load_task_metadata(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Load task metadata from RDS."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # TODO: Query task by ID
                    cursor.execute("""
                        SELECT id, name, priority, status, created_at,
                        updated_at,
                        payload_s3_key, error_message, retry_count
                        FROM tasks
                        WHERE id = %s
                    """, (task_id,))
                    row = cursor.fetchone()
                    if not row:
                        return None
                    # TODO: Return task metadata as dict
                    return {
                        'id': row[0],
                        'name': row[1],
                        'priority': row[2],
                        'status': row[3],
                        'created_at': row[4],
                        'updated_at': row[5],
                        'payload_s3_key': row[6],
                        'error_message': row[7],
                        'retry_count': row[8]

                    }
        except Exception as e:
            logger.error(f"Error loading task {task_id} from RDS: {e}")
            return None

    def update_task_status(self, task_id: str, status: TaskStatus,
                            error_message: Optional[str] = None) -> bool:
        """Update task status in RDS."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # TODO: Update task status and updated_at
                    cursor.execute("""
                        UPDATE tasks
                        SET status = %s, updated_at = CURRENT_TIMESTAMP,
                        error_message = %s
                        WHERE id = %s
                    """, (status.value, error_message, task_id))
                    # TODO: Add execution history entry
                    cursor.execute("""
                        INSERT INTO task_executions (task_id, status,
                        error_message)
                        VALUES (%s, %s, %s)
                    """, (task_id, status.value, error_message))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error updating task {task_id} status: {e}")
            return False

    def get_tasks_by_status(self, status: TaskStatus, limit: int = 100) -> List[Dict]:
        """Get tasks by status from RDS."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # TODO: Query tasks by status with limit
                    cursor.execute("""
                        SELECT id, name, priority, status, created_at,
                        updated_at, payload_s3_key

                        FROM tasks
                        WHERE status = %s
                        ORDER BY priority ASC, created_at ASC
                        LIMIT %s
                    """, (status.value, limit))
                    # TODO: Return list of task metadata
                    tasks = []
                    for row in cursor.fetchall():
                        tasks.append({
                            'id': row[0],
                            'name': row[1],
                            'priority': row[2],
                            'status': row[3],
                            'created_at': row[4],
                            'updated_at': row[5],
                            'payload_s3_key': row[6]
                        })
                    return tasks
        except Exception as e:
            logger.error(f"Error getting tasks by status {status}: {e}")
            return []

    def get_task_statistics(self) -> Dict[str, Any]:
        """Get task processing statistics."""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # TODO: Count tasks by status
                    cursor.execute("""
                        SELECT status, COUNT(*) as count
                        FROM tasks
                        GROUP BY status
                    """)
                    status_counts = {}
                    for row in cursor.fetchall():
                        status_counts[row[0]] = row[1]
                    # TODO: Calculate average processing times
                    cursor.execute("""
                        SELECT AVG(execution_time_ms) as avg_time,

                        MIN(execution_time_ms) as min_time,
                        MAX(execution_time_ms) as max_time
                        FROM task_executions
                        WHERE execution_time_ms IS NOT NULL
                    """)
                    time_stats = cursor.fetchone()
                    return {
                        'status_counts': status_counts,
                        'avg_execution_time_ms': float(time_stats[0]) if
                        time_stats[0] else None,
                        'min_execution_time_ms': time_stats[1],
                        'max_execution_time_ms': time_stats[2]
                    }
        except Exception as e:
            logger.error(f"Error getting task statistics: {e}")
            return {}