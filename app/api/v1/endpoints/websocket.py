"""WebSocket stream for real-time job status updates.

Uses Redis Pub/Sub and FastAPI WebSockets.
"""

import json
import asyncio
import redis.asyncio as redis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.config import settings
from app.core.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


# ===================================================================
# Global Redis (async) connection — reused across all sockets
# ===================================================================
redis_client = redis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
)

# ===================================================================
# Simple Connection Manager
# ===================================================================

class ConnectionManager:
    def __init__(self):
        # job_id → list[WebSocket]
        self.active_connections: dict[int, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, job_id: int):
        await websocket.accept()

        self.active_connections.setdefault(job_id, []).append(websocket)

        logger.info(
            "websocket_connected",
            job_id=job_id,
            client=str(websocket.client),
            total=len(self.active_connections[job_id]),
        )

    def disconnect(self, websocket: WebSocket, job_id: int):
        connections = self.active_connections.get(job_id, [])

        if websocket in connections:
            connections.remove(websocket)

        if not connections:
            self.active_connections.pop(job_id, None)

        logger.info(
            "websocket_disconnected",
            job_id=job_id,
            remaining=len(self.active_connections.get(job_id, [])),
        )

    async def broadcast(self, job_id: int, message: str):
        """Send WS messages to all clients listening on job_id."""
        connections = self.active_connections.get(job_id, [])
        logger.debug(
            "websocket_broadcast",
            job_id=job_id,
            connections=len(connections),
        )

        for ws in list(connections):
            try:
                await ws.send_text(message)
            except Exception:
                # Force disconnect
                self.disconnect(ws, job_id)


manager = ConnectionManager()


# ===================================================================
# WebSocket Endpoint
# ===================================================================

@router.websocket("/ws/jobs/{job_id}/status")
async def websocket_job_status(websocket: WebSocket, job_id: int):
    """
    Stream real-time pipeline job updates.

    - Subscribes to Redis Pub/Sub: job_updates:{job_id}
    - Pushes updates to all connected browser clients
    """
    await manager.connect(websocket, job_id)

    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"job_updates:{job_id}")

    logger.info(
        "redis_subscription_opened",
        channel=f"job_updates:{job_id}",
        job_id=job_id,
    )

    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message:
                logger.debug(
                    "redis_message",
                    channel=f"job_updates:{job_id}",
                    data_preview=str(message.get('data'))[:100],
                )

                # broadcast to all clients of that job
                await manager.broadcast(job_id, message["data"])

            # keep loop efficient
            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        logger.warning(
            "websocket_disconnected_unexpectedly",
            job_id=job_id,
            client=str(websocket.client),
        )
        manager.disconnect(websocket, job_id)

    except Exception as e:
        logger.error(
            "websocket_error",
            job_id=job_id,
            error=str(e),
            exc_info=True,
        )
        manager.disconnect(websocket, job_id)

    finally:
        try:
            await pubsub.unsubscribe(f"job_updates:{job_id}")
            await pubsub.close()
        except Exception:
            pass

        logger.info(
            "websocket_stream_closed",
            job_id=job_id,
        )
