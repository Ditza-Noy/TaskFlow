# event_bus.py
from collections import deque
import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from enum import Enum
import logging
from pydantic import BaseModel

logger = logging.getLogger(__name__)

class EventType(Enum):
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CREATED = "task_created"
    TASK_UPDATED = "task_updated"
    SYSTEM_HEALTH_CHECK = "system_health_check"

class Event(BaseModel):
    id: str
    type: EventType
    timestamp: datetime
    payload: dict[str, Any]
    source: str
    correlation_id: str | None = None

class EventBus:
    """Simple in-memory event bus for TaskFlow system."""
    def __init__(self):
        self.subscribers: dict[EventType, list[Callable[[Event],None]]] = {}
        self.event_history: deque[Event] = deque(maxlen=1000)
        self.lock = threading.Lock()
        self.max_history = 1000 # Keep last 1000 events     

    def subscribe(self, event_type: EventType, handler: Callable[[Event],None]):
        """Subscribe to an event type."""
        # TODO: Implement subscription logic
        # - Use self.lock for thread safety
        # - If event_type not in self.subscribers, initialize empty list
        # - Append handler to self.subscribers[event_type]
        # - Log the subscription
        with self.lock:
            if event_type not in self.subscribers:
                self.subscribers[event_type] = []
            if handler not in self.subscribers[event_type]:
                self.subscribers[event_type].append(handler)
                logger.info(f"Subscribed to {event_type} with handler {handler}")
            else:
                logger.warning(f"Handler {handler} already subscribed to {event_type}")

    def publish(self, event_type: EventType, payload: dict[str, Any],source: str, correlation_id: str | None = None) -> str:
        """Publish an event to the bus."""
        event = Event(
            id=str(uuid.uuid4()),
            type=event_type,
            timestamp=datetime.now(timezone.utc),
            payload=payload,
            source=source,
            correlation_id=correlation_id
        )
        with self.lock:
            self.event_history.append(event)
        self._notify_subscribers(event)
        logger.info(f"Published event {event.id} of type {event.type}")
        return event.id
    
    def _notify_subscribers(self, event: Event):
        """Notify all subscribers of an event."""
        with self.lock:
            handlers = self.subscribers.get(event.type, []).copy()
        for handler in handlers:
            try:
                threading.Thread(target=self._run_handler, args=(handler, event), daemon=True).start()
                logger.info(f"Notified handler {handler} for event {event.id}")
            except Exception as e:
                logger.error(f"Error notifying handler {handler} for event {event.id}: {e}")

    def _run_handler(self, handler: Callable[[Event], None], event: Event):
        """Run event handler with error handling."""
        try:
            handler(event)
        except Exception as e:
            logger.error(f"Error in event handler: {e}")

    def get_events(self, event_type: EventType | None = None, correlation_id: str | None = None, limit: int = 100) -> list[Event]:
        """Get events from history with optional filtering."""
        with self.lock:
            events = list(self.event_history.copy())
        if event_type:
            events = [event for event in events if event.type == event_type]
        if correlation_id:
            events = [event for event in events if event.correlation_id == correlation_id]
        events.sort(key=lambda e: e.timestamp, reverse=True)
        return events[:limit]
    
    def replay_events(self, correlation_id: str):
        """Replay all events for a given correlation ID."""
        events = self.get_events(correlation_id=correlation_id)
        events.sort(key=lambda e: e.timestamp) # Oldest first for replay
        logger.info(f"Replaying {len(events)} events for correlation {correlation_id}")
        for event in events:
            self._notify_subscribers(event)

# Global event bus
event_bus = EventBus()