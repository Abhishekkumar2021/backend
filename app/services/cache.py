"""Redis Cache Service
Handles caching for metadata, credentials, and connection tests
"""

import json
import logging
from typing import Any

import redis

from app.core.config import settings

logger = logging.getLogger(__name__)


class CacheService:
    """Redis-based cache service
    Uses different key prefixes for different data types
    """

    # Key prefixes
    PREFIX_SCHEMA = "schema:"  # Cached schema metadata
    PREFIX_CONFIG = "config:"  # Decrypted connection configs
    PREFIX_TEST = "test:"  # Connection test results
    PREFIX_CONNECTOR = "connector:"  # Connector instances (pickled)
    PREFIX_METADATA = "metadata:"  # General metadata cache

    # TTL values (in seconds)
    TTL_SCHEMA = 3600  # 1 hour - schemas change infrequently
    TTL_CONFIG = 1800  # 30 minutes - session-based
    TTL_TEST = 300  # 5 minutes - test results
    TTL_CONNECTOR = 600  # 10 minutes - connector instances
    TTL_METADATA = 1800  # 30 minutes - general metadata

    def __init__(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.from_url(
                settings.REDIS_URL,
                decode_responses=False,  # We'll handle encoding ourselves
                socket_connect_timeout=5,
                socket_keepalive=True,
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"✅ Redis connected: {settings.REDIS_URL}")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e!s}")
            self.redis_client = None

    def is_available(self) -> bool:
        """Check if Redis is available"""
        if self.redis_client is None:
            return False
        try:
            self.redis_client.ping()
            return True
        except:
            return False

    def _make_key(self, prefix: str, identifier: str) -> str:
        """Create cache key with prefix"""
        return f"{prefix}{identifier}"

    # ============================================
    # SCHEMA METADATA CACHING
    # ============================================

    def get_schema(self, connection_id: int) -> dict[str, Any] | None:
        """Get cached schema metadata

        Args:
            connection_id: Connection ID

        Returns:
            Schema dict or None if not cached

        """
        if not self.is_available():
            return None

        try:
            key = self._make_key(self.PREFIX_SCHEMA, str(connection_id))
            data = self.redis_client.get(key)

            if data:
                logger.info(f"📦 Cache HIT: Schema for connection {connection_id}")
                return json.loads(data.decode("utf-8"))

            logger.debug(f"📦 Cache MISS: Schema for connection {connection_id}")
            return None
        except Exception as e:
            logger.warning(f"Cache read error: {e!s}")
            return None

    def set_schema(self, connection_id: int, schema: dict[str, Any], ttl: int = None) -> bool:
        """Cache schema metadata

        Args:
            connection_id: Connection ID
            schema: Schema dictionary
            ttl: Time to live in seconds (default: TTL_SCHEMA)

        Returns:
            True if cached successfully

        """
        if not self.is_available():
            return False

        try:
            key = self._make_key(self.PREFIX_SCHEMA, str(connection_id))
            data = json.dumps(schema, default=str)
            ttl = ttl or self.TTL_SCHEMA

            self.redis_client.setex(key, ttl, data.encode("utf-8"))
            logger.info(f"💾 Cached schema for connection {connection_id} (TTL: {ttl}s)")
            return True
        except Exception as e:
            logger.warning(f"Cache write error: {e!s}")
            return False

    def invalidate_schema(self, connection_id: int) -> bool:
        """Invalidate cached schema"""
        if not self.is_available():
            return False

        try:
            key = self._make_key(self.PREFIX_SCHEMA, str(connection_id))
            self.redis_client.delete(key)
            logger.info(f"🗑️  Invalidated schema cache for connection {connection_id}")
            return True
        except Exception as e:
            logger.warning(f"Cache delete error: {e!s}")
            return False

    # ============================================
    # DECRYPTED CONFIG CACHING
    # ============================================

    def get_config(self, connection_id: int) -> dict[str, Any] | None:
        """Get cached decrypted config

        Args:
            connection_id: Connection ID

        Returns:
            Config dict or None if not cached

        """
        if not self.is_available():
            return None

        try:
            key = self._make_key(self.PREFIX_CONFIG, str(connection_id))
            data = self.redis_client.get(key)

            if data:
                logger.info(f"📦 Cache HIT: Config for connection {connection_id}")
                return json.loads(data.decode("utf-8"))

            logger.debug(f"📦 Cache MISS: Config for connection {connection_id}")
            return None
        except Exception as e:
            logger.warning(f"Cache read error: {e!s}")
            return None

    def set_config(self, connection_id: int, config: dict[str, Any], ttl: int = None) -> bool:
        """Cache decrypted config

        Args:
            connection_id: Connection ID
            config: Decrypted configuration
            ttl: Time to live in seconds (default: TTL_CONFIG)

        Returns:
            True if cached successfully

        """
        if not self.is_available():
            return False

        try:
            key = self._make_key(self.PREFIX_CONFIG, str(connection_id))
            data = json.dumps(config)
            ttl = ttl or self.TTL_CONFIG

            self.redis_client.setex(key, ttl, data.encode("utf-8"))
            logger.debug(f"💾 Cached config for connection {connection_id} (TTL: {ttl}s)")
            return True
        except Exception as e:
            logger.warning(f"Cache write error: {e!s}")
            return False

    def invalidate_config(self, connection_id: int) -> bool:
        """Invalidate cached config"""
        if not self.is_available():
            return False

        try:
            key = self._make_key(self.PREFIX_CONFIG, str(connection_id))
            self.redis_client.delete(key)
            logger.debug(f"🗑️  Invalidated config cache for connection {connection_id}")
            return True
        except Exception as e:
            logger.warning(f"Cache delete error: {e!s}")
            return False

    # ============================================
    # CONNECTION TEST RESULT CACHING
    # ============================================

    def get_test_result(self, connection_id: int) -> dict[str, Any] | None:
        """Get cached connection test result"""
        if not self.is_available():
            return None

        try:
            key = self._make_key(self.PREFIX_TEST, str(connection_id))
            data = self.redis_client.get(key)

            if data:
                logger.info(f"📦 Cache HIT: Test result for connection {connection_id}")
                return json.loads(data.decode("utf-8"))

            return None
        except Exception as e:
            logger.warning(f"Cache read error: {e!s}")
            return None

    def set_test_result(self, connection_id: int, result: dict[str, Any], ttl: int = None) -> bool:
        """Cache connection test result"""
        if not self.is_available():
            return False

        try:
            key = self._make_key(self.PREFIX_TEST, str(connection_id))
            data = json.dumps(result, default=str)
            ttl = ttl or self.TTL_TEST

            self.redis_client.setex(key, ttl, data.encode("utf-8"))
            logger.debug(f"💾 Cached test result for connection {connection_id} (TTL: {ttl}s)")
            return True
        except Exception as e:
            logger.warning(f"Cache write error: {e!s}")
            return False

    def invalidate_test_result(self, connection_id: int) -> bool:
        """Invalidate cached test result"""
        if not self.is_available():
            return False

        try:
            key = self._make_key(self.PREFIX_TEST, str(connection_id))
            self.redis_client.delete(key)
            return True
        except Exception:
            return False

    # ============================================
    # GENERAL METADATA CACHING
    # ============================================

    def get(self, key: str) -> Any | None:
        """Get generic cached value

        Args:
            key: Cache key

        Returns:
            Cached value or None

        """
        if not self.is_available():
            return None

        try:
            data = self.redis_client.get(self._make_key(self.PREFIX_METADATA, key))
            if data:
                return json.loads(data.decode("utf-8"))
            return None
        except Exception as e:
            logger.warning(f"Cache read error: {e!s}")
            return None

    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set generic cached value

        Args:
            key: Cache key
            value: Value to cache (must be JSON serializable)
            ttl: Time to live in seconds

        Returns:
            True if cached successfully

        """
        if not self.is_available():
            return False

        try:
            cache_key = self._make_key(self.PREFIX_METADATA, key)
            data = json.dumps(value, default=str)
            ttl = ttl or self.TTL_METADATA

            self.redis_client.setex(cache_key, ttl, data.encode("utf-8"))
            return True
        except Exception as e:
            logger.warning(f"Cache write error: {e!s}")
            return False

    def delete(self, key: str) -> bool:
        """Delete cached value"""
        if not self.is_available():
            return False

        try:
            self.redis_client.delete(self._make_key(self.PREFIX_METADATA, key))
            return True
        except Exception:
            return False

    # ============================================
    # CACHE MANAGEMENT
    # ============================================

    def invalidate_connection(self, connection_id: int) -> bool:
        """Invalidate all cached data for a connection
        Call this when connection is updated or deleted
        """
        if not self.is_available():
            return False

        try:
            self.invalidate_schema(connection_id)
            self.invalidate_config(connection_id)
            self.invalidate_test_result(connection_id)
            logger.info(f"🗑️  Invalidated all cache for connection {connection_id}")
            return True
        except Exception as e:
            logger.warning(f"Cache invalidation error: {e!s}")
            return False

    def clear_all(self) -> bool:
        """Clear all application cache (use with caution)"""
        if not self.is_available():
            return False

        try:
            # Delete keys by pattern
            for prefix in [self.PREFIX_SCHEMA, self.PREFIX_CONFIG, self.PREFIX_TEST, self.PREFIX_METADATA]:
                pattern = f"{prefix}*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)

            logger.warning("🗑️  Cleared ALL application cache")
            return True
        except Exception as e:
            logger.error(f"Cache clear error: {e!s}")
            return False

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics"""
        if not self.is_available():
            return {"available": False}

        try:
            info = self.redis_client.info()

            # Count keys by prefix
            key_counts = {}
            for prefix in [self.PREFIX_SCHEMA, self.PREFIX_CONFIG, self.PREFIX_TEST, self.PREFIX_METADATA]:
                pattern = f"{prefix}*"
                key_counts[prefix] = len(self.redis_client.keys(pattern))

            return {
                "available": True,
                "connected_clients": info.get("connected_clients"),
                "used_memory_human": info.get("used_memory_human"),
                "total_keys": sum(key_counts.values()),
                "keys_by_type": key_counts,
            }
        except Exception as e:
            logger.error(f"Stats error: {e!s}")
            return {"available": False, "error": str(e)}


# Global cache service instance
_cache_service: CacheService | None = None


def get_cache() -> CacheService:
    """Get the global cache service instance

    Returns:
        CacheService instance

    """
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service


def reset_cache() -> None:
    """Reset the global cache service (for testing)"""
    global _cache_service
    _cache_service = None
