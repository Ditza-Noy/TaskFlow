# rds_config.py
import os
from psycopg2 import pool
import logging
from dotenv import load_dotenv
from typing import Any
load_dotenv()
logger = logging.getLogger(__name__)
class RDSConfig:
    def __init__(self):
        self.host = os.getenv('RDS_HOST', 'localhost')
        self.port = os.getenv('RDS_PORT', '5432')
        self.database = os.getenv('RDS_DATABASE', 'taskflow')
        self.username = os.getenv('RDS_USERNAME', 'pstgres')
        self.password = os.getenv('RDS_PASSWORD', 'your_password')
        self.min_connections = int(os.getenv('RDS_MIN_CONNECTIONS', '2'))
        self.max_connections = int(os.getenv('RDS_MAX_CONNECTIONS', '10'))
        self.connection_pool: pool.ThreadedConnectionPool | None = None

    def create_connection_pool(self):
        """Create PostgreSQL connection pool."""
        try:
            self.connection_pool = pool.ThreadedConnectionPool(
                self.min_connections,
                self.max_connections,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password)
            logger.info(f"Created RDS connection pool to {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            raise

    def get_connection(self)-> Any:
        if self.connection_pool is None:
            # Either initialize it here or raise a helpful error
            self.create_connection_pool() 
            
        if not self.connection_pool:
            raise Exception("Connection pool failed to initialize.")
            
        return self.connection_pool.getconn()

    def return_connection(self, conn: Any):
        """Return connection to pool."""
        if self.connection_pool:
            self.connection_pool.putconn(conn)

    def close_all_connections(self):
        """Close all connections in pool."""
        if self.connection_pool:
            self.connection_pool.closeall()