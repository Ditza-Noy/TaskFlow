# event_handlers.py
import logging
from event_bus import Event, EventType, event_bus
from typing import  Any

logger = logging.getLogger(__name__)

class TaskEventHandlers:
    """Event handlers for task-related events."""
    def __init__(self):
        # Subscribe to events
        event_bus.subscribe(EventType.TASK_CREATED, self.handle_task_created)
        event_bus.subscribe(EventType.TASK_COMPLETED, self.handle_task_completed)
        event_bus.subscribe(EventType.TASK_FAILED, self.handle_task_failed)

    def handle_task_created(self, event: Event):
        """Handle task created event."""
        task_data = event.payload
        logger.info(f"Task created: {task_data.get('task_id')} -{task_data.get('name')}")
        # TODO: Could trigger notifications, analytics, etc.
        # For now, just log the event
        self._log_task_event("created", event)

    def handle_task_completed(self, event: Event):
        """Handle task completion events."""
        task_data = event.payload
        logger.info(f"Task completed: {task_data.get('task_id')}")
        # TODO: Could trigger follow-up tasks, notifications, cleanup
        self._log_task_event("completed", event)
        # Example: Trigger dependent tasks
        self._trigger_dependent_tasks(task_data)

    def handle_task_failed(self, event: Event):
        """Handle task failure events."""
        task_data = event.payload
        error_message = task_data.get('error_message', 'Unknown error')
        logger.error(f"Task failed: {task_data.get('task_id')} -{error_message}")
        # TODO: Could trigger retry logic, alerts, etc.
        self._log_task_event("failed", event)
        # Example: Trigger retry if below threshold
        self._handle_task_retry(task_data)

    def _log_task_event(self, action: str, event: Event):
        """Log task event for analytics."""
        # This could write to a separate analytics system
        logger.info(f"Task event logged: {action} at {event.timestamp}")        

    def _trigger_dependent_tasks(self, completed_task: dict[str, Any]):
        """Trigger tasks that depend on this completed task."""
        # TODO: Implement dependent task triggering
        # - Check completed_task.get('task_type')
        task_type = completed_task.get('task_type')
        # - If task_type == 'data_processing', publish a new TASK_CREATED event
        if task_type == 'data_processing':
            new_task_payload: dict[str, Any] = {
                'name': f"Report for task {completed_task.get('task_id')}",
                'task_type': 'report_generation',
                'priority': 3,
                'parent_task_id': completed_task.get('task_id')
            }
            event_bus.publish(
                EventType.TASK_CREATED,
                payload=new_task_payload,
                source='dependency_handler',
                correlation_id=completed_task.get('correlation_id')
            )
            logger.info(f"Triggered dependent report generation task for {completed_task.get('task_id')}")

    def _handle_task_retry(self, failed_task: dict[str, Any]):
        """
        Handle retry logic for failed tasks.
        If the current retry count is less than max_retries, 
        publish a new TASK_CREATED event with an incremented counter.
        """
        # 1. Extract retry information from the failed task metadata
        # These fields correspond to the RDS 'tasks' table schema
        retry_count = failed_task.get('retry_count', 0)
        max_retries = failed_task.get('max_retries', 3)

        # 2. Check if the task is eligible for another attempt
        if retry_count < max_retries:
            new_retry_count = retry_count + 1
            
            logger.info(
                f"Retry initiated for task {failed_task.get('task_id')}: "
                f"Attempt {new_retry_count} of {max_retries}"
            )

            # 3. Publish a new TASK_CREATED event to the EventBus
            # This will eventually trigger the worker to try the task again
            event_bus.publish(
                event_type=EventType.TASK_CREATED,
                payload={
                    'task_id': failed_task.get('task_id'),
                    'name': failed_task.get('name'),
                    'priority': failed_task.get('priority'),
                    'payload': failed_task.get('payload'),
                    'retry_count': new_retry_count,
                    'max_retries': max_retries,
                    'original_failure': failed_task.get('error_message')
                },
                source='retry_handler',
                correlation_id=failed_task.get('correlation_id')
            )
        else:
            # Final failure state after all retries are exhausted
            logger.error(
                f"Task {failed_task.get('task_id')} reached max retries ({max_retries}). "
                "Abandoning task."
            )

class SystemEventHandlers:
    """Event handlers for system-level events."""
    def __init__(self):
        event_bus.subscribe(EventType.SYSTEM_HEALTH_CHECK, self.handle_health_check)
    def handle_health_check(self, event: Event):
        """Handle system health check events."""
        health_data = event.payload
        # Check if any services are unhealthy
        unhealthy_services: list[str]= []
        for service, health in health_data.items():
            if not health.get('healthy', True):
                unhealthy_services.append(service)
        if unhealthy_services:
            logger.warning(f"Unhealthy services detected:{unhealthy_services}")
            
# Could trigger alerts, auto-healing, etc.
# Initialize event handlers
task_handlers = TaskEventHandlers()
system_handlers = SystemEventHandlers()