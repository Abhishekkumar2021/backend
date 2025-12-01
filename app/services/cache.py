"""Redis Cache Service
Handles caching for metadata, credentials, and connection tests
"""

import json
from typing import Any
import redis

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class CacheService:
    """Redis-based cache service with structured logging."""

    # Key prefixes
    PREFIX_SCHEMA = "schema:"
    PREFIX_CONFIG = "config:"
    PREFIX_TEST = "test:"
    PREFIX_CONNECTOR = "connector:"
    PREFIX_METADATA = "metadata:"

    # TTL values
    TTL_SCHEMA = 3600
    TTL_CONFIG = 1800
    TTL_TEST = 300
    TTL_CONNECTOR = 600
    TTL_METADATA = 1800

    # ===================================================================
    # Initialization
    # ===================================================================
    def __init__(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.from_url(
                settings.REDIS_URL,
                decode_responses=False,
                socket_connect_timeout=5,
                socket_keepalive=True,
            )

            # Test connection
            self.redis_client.ping()

            logger.info(
                "cache_connection_successful",
                redis_url=settings.REDIS_URL,
            )

        except Exception as e:
            logger.error(
                "cache_connection_failed",
                error=str(e),
                redis_url=settings.REDIS_URL,
                exc_info=True,
            )
            self.redis_client = None

    def is_available(self) -> bool:
        """Check if Redis is available."""
        if self.redis_client is None:
            return False

        try:
            self.redis_client.ping()
            return True
        except Exception:
            return False

    def _make_key(self, prefix: str, identifier: str) -> str:
        """Construct namespaced key."""
        return f"{prefix}{identifier}"

    # ===================================================================
    # SCHEMA CACHING
    # ===================================================================
    def get_schema(self, connection_id: int) -> dict[str, Any] | None:
        if not self.is_available():
            return None

        key = self._make_key(self.PREFIX_SCHEMA, str(connection_id))

        try:
            data = self.redis_client.get(key)

            if data:
                logger.debug(
                    "schema_cache_hit",
                    connection_id=connection_id,
                )
                return json.loads(data.decode("utf-8"))

            logger.debug(
                "schema_cache_miss",
                connection_id=connection_id,
            )
            return None

        except Exception as e:
            logger.warning(
                "schema_cache_read_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return None

    def set_schema(self, connection_id: int, schema: dict[str, Any], ttl: int = None) -> bool:
        if not self.is_available():
            return False

        key = self._make_key(self.PREFIX_SCHEMA, str(connection_id))
        ttl = ttl or self.TTL_SCHEMA

        try:
            payload = json.dumps(schema)
            self.redis_client.setex(key, ttl, payload.encode("utf-8"))

            logger.info(
                "schema_cache_write_success",
                connection_id=connection_id,
                ttl=ttl,
            )
            return True

        except Exception as e:
            logger.warning(
                "schema_cache_write_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return False

    def invalidate_schema(self, connection_id: int) -> bool:
        if not self.is_available():
            return False

        key = self._make_key(self.PREFIX_SCHEMA, str(connection_id))

        try:
            self.redis_client.delete(key)
            logger.info(
                "schema_cache_invalidated",
                connection_id=connection_id,
            )
            return True
        except Exception as e:
            logger.warning(
                "schema_cache_invalidation_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return False

    # ===================================================================
    # CONFIG CACHING
    # ===================================================================
    def get_config(self, connection_id: int) -> dict[str, Any] | None:
        if not self.is_available():
            return None

        key = self._make_key(self.PREFIX_CONFIG, str(connection_id))

        try:
            data = self.redis_client.get(key)

            if data:
                logger.debug(
                    "config_cache_hit",
                    connection_id=connection_id,
                )
                return json.loads(data.decode("utf-8"))

            logger.debug(
                "config_cache_miss",
                connection_id=connection_id,
            )
            return None

        except Exception as e:
            logger.warning(
                "config_cache_read_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return None

    def set_config(self, connection_id: int, config: dict[str, Any], ttl: int = None) -> bool:
        if not self.is_available():
            return False

        key = self._make_key(self.PREFIX_CONFIG, str(connection_id))
        ttl = ttl or self.TTL_CONFIG

        try:
            payload = json.dumps(config)
            self.redis_client.setex(key, ttl, payload.encode("utf-8"))

            logger.info(
                "config_cache_write_success",
                connection_id=connection_id,
                ttl=ttl,
            )
            return True

        except Exception as e:
            logger.warning(
                "config_cache_write_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return False

    def invalidate_config(self, connection_id: int) -> bool:
        if not self.is_available():
            return False

        key = self._make_key(self.PREFIX_CONFIG, str(connection_id))

        try:
            self.redis_client.delete(key)
            logger.info(
                "config_cache_invalidated",
                connection_id=connection_id,
            )
            return True
        except Exception as e:
            logger.warning(
                "config_cache_invalidation_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return False

    # ===================================================================
    # TEST RESULT CACHING
    # ===================================================================
    def get_test_result(self, connection_id: int) -> dict[str, Any] | None:
        if not self.is_available():
            return None

        key = self._make_key(self.PREFIX_TEST, str(connection_id))

        try:
            data = self.redis_client.get(key)

            if data:
                logger.debug(
                    "test_result_cache_hit",
                    connection_id=connection_id,
                )
                return json.loads(data.decode("utf-8"))
            return None

        except Exception as e:
            logger.warning(
                "test_result_cache_read_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return None

    def set_test_result(self, connection_id: int, result: dict[str, Any], ttl: int = None) -> bool:
        if not self.is_available():
            return False

        key = self._make_key(self.PREFIX_TEST, str(connection_id))
        ttl = ttl or self.TTL_TEST

        try:
            payload = json.dumps(result)
            self.redis_client.setex(key, ttl, payload.encode("utf-8"))

            logger.debug(
                "test_result_cache_write_success",
                connection_id=connection_id,
                ttl=ttl,
            )
            return True

        except Exception as e:
            logger.warning(
                "test_result_cache_write_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return False

    def invalidate_test_result(self, connection_id: int) -> bool:
        if not self.is_available():
            return False

        key = self._make_key(self.PREFIX_TEST, str(connection_id))

        try:
            self.redis_client.delete(key)
            logger.info(
                "test_result_cache_invalidated",
                connection_id=connection_id,
            )
            return True
        except Exception as e:
            logger.warning(
                "test_result_cache_invalidation_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return False

    # ===================================================================
    # GENERIC METADATA CACHING
    # ===================================================================
    def get(self, key: str) -> Any | None:
        if not self.is_available():
            return None

        cache_key = self._make_key(self.PREFIX_METADATA, key)

        try:
            data = self.redis_client.get(cache_key)
            if data:
                logger.debug("metadata_cache_hit", key=key)
                return json.loads(data.decode("utf-8"))
            logger.debug("metadata_cache_miss", key=key)
            return None

        except Exception as e:
            logger.warning(
                "metadata_cache_read_error",
                key=key,
                error=str(e),
                exc_info=True,
            )
            return None

    def set(self, key: str, value: Any, ttl: int = None) -> bool:
        if not self.is_available():
            return False

        cache_key = self._make_key(self.PREFIX_METADATA, key)
        ttl = ttl or self.TTL_METADATA

        try:
            payload = json.dumps(value)
            self.redis_client.setex(cache_key, ttl, payload.encode("utf-8"))

            logger.debug(
                "metadata_cache_write_success",
                key=key,
                ttl=ttl,
            )
            return True

        except Exception as e:
            logger.warning(
                "metadata_cache_write_error",
                key=key,
                error=str(e),
                exc_info=True,
            )
            return False

    def delete(self, key: str) -> bool:
        if not self.is_available():
            return False

        cache_key = self._make_key(self.PREFIX_METADATA, key)

        try:
            self.redis_client.delete(cache_key)
            logger.debug(
                "metadata_cache_deleted",
                key=key,
            )
            return True

        except Exception as e:
            logger.warning(
                "metadata_cache_delete_error",
                key=key,
                error=str(e),
                exc_info=True,
            )
            return False

    # ===================================================================
    # CONNECTION-LEVEL INVALIDATION
    # ===================================================================
    def invalidate_connection(self, connection_id: int) -> bool:
        if not self.is_available():
            return False

        try:
            self.invalidate_schema(connection_id)
            self.invalidate_config(connection_id)
            self.invalidate_test_result(connection_id)

            logger.info(
                "connection_cache_invalidated",
                connection_id=connection_id,
            )
            return True
        except Exception as e:
            logger.warning(
                "connection_cache_invalidation_error",
                connection_id=connection_id,
                error=str(e),
                exc_info=True,
            )
            return False

    # ===================================================================
    # CLEAR ALL CACHE
    # ===================================================================
    def clear_all(self) -> bool:
        if not self.is_available():
            return False

        try:
            prefixes = [
                self.PREFIX_SCHEMA,
                self.PREFIX_CONFIG,
                self.PREFIX_TEST,
                self.PREFIX_METADATA,
            ]

            for prefix in prefixes:
                pattern = f"{prefix}*"
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)

            logger.warning("all_cache_cleared")
            return True

        except Exception as e:
            logger.error(
                "all_cache_clear_error",
                error=str(e),
                exc_info=True,
            )
            return False

    # ===================================================================
    # STATS
    # ===================================================================
    def get_stats(self) -> dict[str, Any]:
        if not self.is_available():
            return {"available": False}

        try:
            info = self.redis_client.info()

            key_counts = {}
            prefixes = [
                self.PREFIX_SCHEMA,
                self.PREFIX_CONFIG,
                self.PREFIX_TEST,
                self.PREFIX_METADATA,
            ]

            for prefix in prefixes:
                pattern = f"{prefix}*"
                key_counts[prefix] = len(self.redis_client.keys(pattern))

            logger.debug("cache_stats_retrieved")

            return {
                "available": True,
                "connected_clients": info.get("connected_clients"),
                "used_memory_human": info.get("used_memory_human"),
                "total_keys": sum(key_counts.values()),
                "keys_by_type": key_counts,
            }

        except Exception as e:
            logger.error(
                "cache_stats_error",
                error=str(e),
                exc_info=True,
            )
            return {"available": False, "error": str(e)}


# ===================================================================
# GLOBAL CACHE SINGLETON
# ===================================================================
_cache_service: CacheService | None = None


def get_cache() -> CacheService:
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service


def reset_cache() -> None:
    global _cache_service
    _cache_service = None