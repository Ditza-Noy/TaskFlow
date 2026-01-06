# task_cache.py
import threading
import time
from typing import Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class CacheEntry:
    """Represents a single cache entry with expiration."""
    def __init__(self, data: Any, ttl_seconds: int = 300):
        self.data = data
        self.creation_time = datetime.now()
        self.ttl_seconds = ttl_seconds
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return datetime.now() > self.creation_time + timedelta(seconds=self.ttl_seconds)
    def get_age_seconds(self) -> float:
        """Get the age of the cache entry in seconds."""
        return (datetime.now() - self.creation_time).total_seconds()
    
class TaskCache:
    """Simple in-memory cache for task data using cache-aside pattern."""
    def __init__(self,default_ttl: int = 300):
        self.cache: dict[str, CacheEntry] = {}
        self.lock = threading.Lock()
        self.default_ttl = default_ttl

        # Metrics
        self.stats = {
        'hits': 0,
        'misses': 0,
        'sets': 0,
        'deletes': 0,
        'evictions': 0
        }
        # Start cleanup thread
        self._start_cleanup_thread()

    def get(self, key: str) -> Any | None:
        """Get value from cache (cache-aside pattern)."""
        # TODO: Implement cache get with expiration handling
        with self.lock:
            self.stats['evictions'] += 1
            if key in self.cache:
                entry = self.cache[key]
                if entry.is_expired():
                    del self.cache[key]
                    self.stats['misses'] += 1
                    logger.debug(f"Cache miss (expired) for key: {key}")
                    return None
                else:
                    self.stats['hits'] += 1
                    age = entry.get_age_seconds()
                    logger.debug(f"Cache hit for key: {key}, age: {age:.2f} seconds")
                    return entry.data
            else:
                self.stats['misses'] += 1
                logger.debug(f"Cache miss for key: {key}")
                return None
        
    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        """Set value in cache with optional TTL."""
        ttl_seconds = ttl_seconds or self.default_ttl
        with self.lock:
            self.cache[key] = CacheEntry(value, ttl_seconds)
            self.stats['sets'] += 1
            logger.debug(f"Cache set for key: {key} with TTL: {ttl_seconds} seconds")

    def delete(self, key: str) -> bool:
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                self.stats['deletes'] += 1
                logger.debug(f"Cache delete for key: {key}")
                return True
            return False
        
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern."""
        # TODO: Implement pattern-based invalidation
        # 1. Use self.lock for thread safety
        # 2. Find all keys that contain the pattern string
        # 3. Delete each matching key and increment stats['deletes']
        # 4. Log debug message with pattern and count of deleted keys
        # 5. Return the count of deleted keys
        with self.lock:
            keys_to_delete = [key for key in self.cache if pattern in key]
            for key in keys_to_delete:
                del self.cache[key]
                self.stats['deletes'] += 1
            logger.debug(f"Cache invalidate pattern: {pattern}, deleted keys count: {len(keys_to_delete)}")
            return len(keys_to_delete)
        
    def clear(self) -> None:
        """Clear all cache entries."""
        with self.lock:
            count = len(self.cache)
            self.cache.clear()
            self.stats['deletes'] += count
            logger.info(f"Cache cleared: {count} entries")

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = (self.stats['hits'] / total_requests * 100) if total_requests > 0 else 0
            return {
                'entries': len(self.cache),
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'hit_rate_percent': round(hit_rate, 2),
                'sets': self.stats['sets'],
                'deletes': self.stats['deletes'],
                'evictions': self.stats['evictions']
            }
        
    def _cleanup_expired(self):
        """Remove expired entries from cache."""
        with self.lock:
            expired_keys = [key for key, entry in self.cache.items() if entry.is_expired()]
            for key in expired_keys:
                del self.cache[key]
                self.stats['evictions'] += 1
            if expired_keys:
                logger.info(f"Cache cleanup: evicted {len(expired_keys)} expired entries")

    def _start_cleanup_thread(self):
        """Start background thread for cache cleanup."""
        def cleanup_loop():
            while True:
                time.sleep(60) # Cleanup every minute
                try:
                    self._cleanup_expired()
                except Exception as e:
                    logger.error(f"Error in cache cleanup: {e}")
        cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        cleanup_thread.start()
# Global cache instance
task_cache = TaskCache()