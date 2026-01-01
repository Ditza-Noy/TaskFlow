# protected_clients.py
import logging
from circuit_breaker import circuit_breaker_manager, CircuitBreakerError, CircuitBreakerConfig
from rds_config import RDSConfig
from s3_config import S3Config
from sqs_config import SQSConfig
from typing import Any


logger = logging.getLogger(__name__)

class ProtectedS3Client:
    """S3 client with circuit breaker protection."""
    def __init__(self):
        self.s3_config = S3Config()
        self.s3_client = self.s3_config.get_s3_client()
        self.circuit_breaker = circuit_breaker_manager.get_circuit_breaker('S3', CircuitBreakerConfig(failure_threshold=3, timeout_seconds=30))
    def put_object(self, **kwargs: Any ):
        """Put object with circuit breaker protection."""
        # TODO: Implement circuit breaker protected put_object
        # - Use self.circuit_breaker.call() to wrap self.s3_client.put_object
        # - Catch CircuitBreakerError and log it, then return fallback response
        # - Use self._get_fallback_response('put_object') for fallback
        try:
            return self.circuit_breaker.call(self.s3_client.put_object, **kwargs)
        except CircuitBreakerError:
            logger.error("S3 put_object blocked by circuit breaker")
            return self._get_fallback_response('put_object')
        
    def get_object(self, **kwargs: Any):
        """Get object with circuit breaker protection."""
        try:
            return self.circuit_breaker.call(self.s3_client.get_object, **kwargs)
        except CircuitBreakerError:
            logger.error("S3 get_object blocked by circuit breaker")
            return self._get_fallback_response('get_object')
    def delete_object(self, **kwargs: Any ):
        """Delete object with circuit breaker protection."""
        try:
            return self.circuit_breaker.call(self.s3_client.delete_object,**kwargs)
        except CircuitBreakerError:
            logger.error("S3 delete_object blocked by circuit breaker")
            return self._get_fallback_response('delete_object')
    def _get_fallback_response(self, operation: str):
        """Provide fallback response when circuit is open."""
        # Could return cached data, default response, or raise service unavailable
        raise Exception(f"S3 service temporarily unavailable ({operation})")
    
    class ProtectedSQSClient:
        """SQS client with circuit breaker protection."""
        def __init__(self):
            self.sqs_config = SQSConfig()
            self.sqs_client = self.sqs_config.get_sqs_client()
            self.circuit_breaker = circuit_breaker_manager.get_circuit_breaker('SQS', CircuitBreakerConfig(failure_threshold=5, timeout_seconds=45))
            
        def send_message(self, **kwargs: Any):
            """Send message with circuit breaker protection."""
            try:
                return self.circuit_breaker.call(self.sqs_client.send_message, **kwargs)
            except CircuitBreakerError:
                logger.error("SQS send_message blocked by circuit breaker")
                # Could queue messages locally for later retry
                raise Exception("SQS service temporarily unavailable")
        def receive_message(self, **kwargs: Any)-> dict[str, Any]:
            """Receive message with circuit breaker protection."""
            try:
                return self.circuit_breaker.call(self.sqs_client.receive_message, **kwargs)
            except CircuitBreakerError:
                logger.error("SQS receive_message blocked by circuit breaker")
                return {'Messages': []} # Return empty result
    class ProtectedRDSConnection:
        """RDS connection with circuit breaker protection."""
        def __init__(self):
            self.rds_config = RDSConfig()
            self.circuit_breaker = circuit_breaker_manager.get_circuit_breaker('RDS', CircuitBreakerConfig(failure_threshold=3, timeout_seconds=60))
        def execute_query(self, query: str, params: tuple[Any, ...] | None = None)-> Any:
            """Execute query with circuit breaker protection."""
            def _execute()-> Any:
                conn = self.rds_config.get_connection()
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(query, params)

                    if query.strip().upper().startswith('SELECT'): 
                        return cursor.fetchall()
                    else:
                        conn.commit()
                        return cursor.rowcount
                finally:
                    self.rds_config.return_connection(conn)

            try:
                return self.circuit_breaker.call(_execute)
            except CircuitBreakerError:
                logger.error("RDS query blocked by circuit breaker")
                # Could return cached data or raise service unavailable
                raise Exception("Database service temporarily unavailable")
 