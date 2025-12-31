# logging_config.py
import logging
import json
import sys
import types
from datetime import datetime, timezone
from typing import Any
import uuid

class JSONFormatter(logging.Formatter):
    """Custom formatter for JSON logging."""
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry: dict[str, Any] = {
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # --- NEW CODE STARTS HERE ---
        # 1. Define the list of extra fields you want to capture
        custom_fields = [
            'request_id', 'task_id', 'duration_ms', 'status_code',
            'server_port', 'method', 'url', 'client_ip', 'user_agent', 'event'
        ]

        # 2. Loop through them and add to log_entry if they exist in the record
        for field in custom_fields:
            if hasattr(record, field):
                log_entry[field] = getattr(record, field)
        # --- NEW CODE ENDS HERE ---

        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_entry)
    
def setup_logging(log_level: str = "INFO", use_json: bool = True):
        """Configure application logging."""
        # Clear any existing handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        if use_json:
            handler.setFormatter(JSONFormatter())
        else:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
        # Configure root logger
        root_logger.addHandler(handler)
        root_logger.setLevel(getattr(logging, log_level.upper()))
    
class APIRequestLogger:
    """Context manager for logging API requests."""
    def __init__(self, endpoint: str, method: str, request_id: str | None = None):
        self.endpoint = endpoint
        self.method = method
        self.request_id = request_id or str(uuid.uuid4())
        self.start_time = None
        self.logger = logging.getLogger("api.requests")

    def __enter__(self):
        self.start_time = datetime.now(timezone.utc)
        self.logger.info(
            f"API request started: {self.method} {self.endpoint}",
            extra={
                'request_id': self.request_id,
                'endpoint': self.endpoint,
                'method': self.method,
                'event': 'request_start'
            }
        )
        return self
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: types.TracebackType | None) -> None:
        duration = None
        if self.start_time:
            duration = (datetime.now(timezone.utc) - self.start_time).total_seconds() * 1000
        if exc_type:
            self.logger.error(
                f"API request failed: {self.method} {self.endpoint}",
                extra={
                    'request_id': self.request_id,
                    'endpoint': self.endpoint,
                    'method': self.method,
                    'duration_ms': duration,
                    'event': 'request_error',
                    'error': str(exc_val)
                }
            )
        else:
            self.logger.info(
                f"API request completed: {self.method} {self.endpoint}",
                extra={
                    'request_id': self.request_id,
                    'endpoint': self.endpoint,
                    'method': self.method,
                    'duration_ms': duration,
                    'event': 'request_complete'
                }
            )
        