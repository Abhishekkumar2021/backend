# Redis Caching Integration

## Overview
Redis is used to cache metadata, configuration, and connection test results to improve performance and reduce load on data sources.

### What is Cached?
1.  **Schema Metadata:** (1 hour TTL) - Avoids re-scanning databases for every ERD view.
2.  **Decrypted Config:** (30 min TTL) - Avoids repeated decryption operations.
3.  **Connection Test Results:** (5 min TTL) - Prevents hammering databases with health checks.
4.  **Generic Metadata:** (30 min TTL) - Flexible application caching.

## Configuration
To enable caching, ensure Redis is installed and configured in your `.env` file:

```env
REDIS_URL=redis://localhost:6379/0
```

## Cache Invalidation Strategy
-   **Automatic:**
    -   **Connection Update:** Clears schema, config, and test results.
    -   **Connection Delete:** Clears all associated cache.
    -   **TTL Expiration:** Redis automatically removes stale keys.
-   **Manual:**
    -   API endpoint to invalidate cache for a specific connection.
    -   Admin endpoint to clear global cache.

## API Endpoints for Caching
-   `GET /api/v1/cache/stats`: View Redis statistics.
-   `GET /api/v1/cache/health`: Check Redis health.
-   `POST /api/v1/cache/clear`: Clear all cache.
-   `POST /api/v1/connections/{id}/cache/invalidate`: Invalidate cache for a connection.

## Performance Impact
| Operation | Without Cache | With Cache |
|-----------|---------------|------------|
| Schema Discovery | 2-5s | <10ms |
| Connection Test | 100-300ms | <5ms |
| Config Decryption | 50ms | <5ms |
