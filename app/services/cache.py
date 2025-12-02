"""
Redis Cache Service (Improved & Hardened)
-----------------------------------------
Unified caching for:
✓ Schema
✓ Config
✓ Test results
✓ Metadata
✓ Connector data

Enhancements:
✓ Non-blocking SCAN instead of KEYS
✓ Consistent binary-safe serialization
✓ Connection-check guards
✓ Centralized JSON encode/decode
✓ Stronger structured logging
✓ Namespace isolation
✓ Simplified common logic
"""

import json
from typing import Any, Optional, Callable, Dict

import redis

from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)


# ================================================================
# Utility — JSON helpers (binary safe)
# ================================================================
def _encode(value: Any) -> bytes:
    try:
        return json.dumps(value).encode("utf-8")
    except Exception:
        # Fallback for non-JSON-safe objects
        return str(value).encode("utf-8")


def _decode(raw: Optional[bytes]) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        # fallback: return raw string if not JSON
        return raw.decode("utf-8")


# ================================================================
# Cache Service
# ================================================================
class CacheService:
    """Robust Redis-based caching layer."""

    # Namespaces
    PREFIX = {
        "schema": "schema:",
        "config": "config:",
        "test": "test:",
        "metadata": "metadata:",
        "connector": "connector:",
    }

    # TTLs
    TTL = {
        "schema": 3600,
        "config": 1800,
        "test": 300,
        "metadata": 1800,
        "connector": 600,
    }

    # ------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------
    def __init__(self):
        try:
            self.client = redis.from_url(
                settings.REDIS_URL,
                decode_responses=False,
                socket_connect_timeout=3,
                socket_keepalive=True,
            )

            self.client.ping()
            logger.info("cache_connected", redis_url=settings.REDIS_URL)

        except Exception as e:
            logger.error(
                "cache_connection_failed",
                redis_url=settings.REDIS_URL,
                error=str(e),
                exc_info=True,
            )
            self.client = None

    # ------------------------------------------------------------
    # Redis availability
    # ------------------------------------------------------------
    def is_available(self) -> bool:
        if not self.client:
            return False
        try:
            self.client.ping()
            return True
        except Exception:
            return False

    # ------------------------------------------------------------
    # Key builder
    # ------------------------------------------------------------
    def _key(self, ns: str, identifier: str) -> str:
        return f"{self.PREFIX[ns]}{identifier}"

    # ------------------------------------------------------------
    # Low-level getters/setters
    # ------------------------------------------------------------
    def _get(self, key: str) -> Any:
        if not self.is_available():
            return None

        try:
            raw = self.client.get(key)
            if raw:
                logger.debug("cache_hit", key=key)
                return _decode(raw)

            logger.debug("cache_miss", key=key)
            return None

        except Exception as e:
            logger.warning("cache_get_error", key=key, error=str(e))
            return None

    def _set(self, key: str, value: Any, ttl: int) -> bool:
        if not self.is_available():
            return False

        try:
            self.client.setex(key, ttl, _encode(value))
            logger.debug("cache_set_success", key=key, ttl=ttl)
            return True
        except Exception as e:
            logger.warning("cache_set_error", key=key, error=str(e))
            return False

    def _delete(self, key: str) -> bool:
        if not self.is_available():
            return False

        try:
            self.client.delete(key)
            logger.debug("cache_delete_success", key=key)
            return True
        except Exception as e:
            logger.warning("cache_delete_error", key=key, error=str(e))
            return False

    # ------------------------------------------------------------
    # SCAN wrapper (non-blocking alternative to KEYS)
    # ------------------------------------------------------------
    def _scan_prefix(self, prefix: str) -> list[str]:
        """Efficiently get all keys for a prefix using SCAN."""
        if not self.is_available():
            return []

        keys = []
        cursor = 0
        pattern = f"{prefix}*"

        try:
            while True:
                cursor, batch = self.client.scan(cursor, match=pattern, count=500)
                keys.extend(batch)
                if cursor == 0:
                    break
            return keys

        except Exception as e:
            logger.warning("cache_scan_error", prefix=prefix, error=str(e))
            return []

    # ================================================================
    # Typed Caches
    # ================================================================
    # ---------------- Schema ----------------
    def get_schema(self, connection_id: int):
        return self._get(self._key("schema", str(connection_id)))

    def set_schema(self, connection_id: int, value: Any):
        return self._set(self._key("schema", str(connection_id)), value, self.TTL["schema"])

    def invalidate_schema(self, connection_id: int):
        return self._delete(self._key("schema", str(connection_id)))

    # ---------------- Config ----------------
    def get_config(self, connection_id: int):
        return self._get(self._key("config", str(connection_id)))

    def set_config(self, connection_id: int, value: Any):
        return self._set(self._key("config", str(connection_id)), value, self.TTL["config"])

    def invalidate_config(self, connection_id: int):
        return self._delete(self._key("config", str(connection_id)))

    # ---------------- Test Result ----------------
    def get_test_result(self, connection_id: int):
        return self._get(self._key("test", str(connection_id)))

    def set_test_result(self, connection_id: int, value: Any):
        return self._set(self._key("test", str(connection_id)), value, self.TTL["test"])

    def invalidate_test_result(self, connection_id: int):
        return self._delete(self._key("test", str(connection_id)))

    # ---------------- Generic Metadata ----------------
    def get(self, key: str):
        return self._get(self._key("metadata", key))

    def set(self, key: str, value: Any):
        return self._set(self._key("metadata", key), value, self.TTL["metadata"])

    def delete(self, key: str):
        return self._delete(self._key("metadata", key))

    # ------------------------------------------------------------
    # Invalidate all cache for a connection
    # ------------------------------------------------------------
    def invalidate_connection(self, connection_id: int) -> bool:
        if not self.is_available():
            return False

        ok1 = self.invalidate_schema(connection_id)
        ok2 = self.invalidate_config(connection_id)
        ok3 = self.invalidate_test_result(connection_id)

        logger.info(
            "connection_cache_invalidated",
            connection_id=connection_id,
            schema=ok1,
            config=ok2,
            test=ok3,
        )

        return ok1 and ok2 and ok3

    # ------------------------------------------------------------
    # Clear all cache (safe, non-blocking)
    # ------------------------------------------------------------
    def clear_all(self) -> bool:
        if not self.is_available():
            return False

        try:
            for prefix in self.PREFIX.values():
                keys = self._scan_prefix(prefix)
                if keys:
                    self.client.delete(*keys)

            logger.warning("cache_cleared_all")
            return True

        except Exception as e:
            logger.error("cache_clear_all_error", error=str(e))
            return False

    # ------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------
    def get_stats(self) -> dict[str, Any]:
        if not self.is_available():
            return {"available": False}

        try:
            info = self.client.info()

            prefix_counts = {
                p: len(self._scan_prefix(pref))
                for p, pref in self.PREFIX.items()
            }

            return {
                "available": True,
                "connected_clients": info.get("connected_clients"),
                "used_memory_human": info.get("used_memory_human"),
                "keys_by_prefix": prefix_counts,
                "total_keys": sum(prefix_counts.values()),
            }

        except Exception as e:
            logger.error("cache_stats_error", error=str(e))
            return {"available": False, "error": str(e)}


# ------------------------------------------------------------
# Global Singleton
# ------------------------------------------------------------
_cache_instance: Optional[CacheService] = None


def get_cache() -> CacheService:
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = CacheService()
    return _cache_instance


def reset_cache():
    """Useful for testing."""
    global _cache_instance
    _cache_instance = None
