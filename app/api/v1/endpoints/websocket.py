import asyncio
import json

import redis.asyncio as redis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.config import settings
from app.core.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


# ===================================================================
# Simple Connection Manager
# ===================================================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[int, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, job_id: int):
        logger.info(
            "websocket_connection_requested",
            job_id=job_id,
            client=str(websocket.client),
        )
        await websocket.accept()

        if job_id not in self.active_connections:
            self.active_connections[job_id] = []

        self.active_connections[job_id].append(websocket)

        logger.info(
            "websocket_client_connected",
            job_id=job_id,
            total_connections=len(self.active_connections[job_id]),
        )

    def disconnect(self, websocket: WebSocket, job_id: int):
        if job_id in self.active_connections:
            try:
                self.active_connections[job_id].remove(websocket)
            except ValueError:
                logger.warning(
                    "websocket_disconnect_missing_client",
                    job_id=job_id,
                    client=str(websocket.client),
                )

            if not self.active_connections[job_id]:
                del self.active_connections[job_id]

        logger.info(
            "websocket_client_disconnected",
            job_id=job_id,
        )

    async def send_personal_message(self, message: str, websocket: WebSocket):
        logger.debug(
            "websocket_send_personal_message",
            client=str(websocket.client),
            message_preview=message[:50],
        )
        await websocket.send_text(message)

    async def broadcast(self, message: str, job_id: int):
        logger.debug(
            "websocket_broadcast",
            job_id=job_id,
            connections=len(self.active_connections.get(job_id, [])),
        )
        if job_id in self.active_connections:
            for connection in self.active_connections[job_id]:
                await connection.send_text(message)


manager = ConnectionManager()


# ===================================================================
# WebSocket Endpoint
# ===================================================================

@router.websocket("/ws/jobs/{job_id}/status")
async def websocket_endpoint(websocket: WebSocket, job_id: int):
    logger.info(
        "websocket_job_status_stream_started",
        job_id=job_id,
        client=str(websocket.client),
    )

    await manager.connect(websocket, job_id)

    r = redis.from_url(settings.REDIS_URL, decode_responses=True)
    pubsub = r.pubsub()

    await pubsub.subscribe(f"job_updates:{job_id}")
    logger.info(
        "redis_subscribed_to_job_channel",
        channel=f"job_updates:{job_id}",
    )

    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message:
                logger.debug(
                    "redis_pubsub_message_received",
                    job_id=job_id,
                    message_preview=str(message["data"])[:80],
                )
                await manager.send_personal_message(message["data"], websocket)

            # Keep the websocket alive
            ping_msg = json.dumps({"type": "ping"})
            await websocket.send_text(ping_msg)
            logger.debug(
                "websocket_ping_sent",
                job_id=job_id,
            )

            await asyncio.sleep(5)

    except WebSocketDisconnect:
        logger.warning(
            "websocket_client_disconnected_unexpectedly",
            job_id=job_id,
            client=str(websocket.client),
        )
        manager.disconnect(websocket, job_id)

        await pubsub.unsubscribe(f"job_updates:{job_id}")
        await pubsub.close()

    except asyncio.CancelledError:
        logger.warning(
            "websocket_cancelled",
            job_id=job_id,
        )
        manager.disconnect(websocket, job_id)

        await pubsub.unsubscribe(f"job_updates:{job_id}")
        await pubsub.close()

    except Exception as e:
        logger.error(
            "websocket_unexpected_error",
            job_id=job_id,
            error=str(e),
            exc_info=True,
        )
        manager.disconnect(websocket, job_id)

        await pubsub.unsubscribe(f"job_updates:{job_id}")
        await pubsub.close()

    finally:
        logger.info(
            "websocket_job_status_stream_closed",
            job_id=job_id,
        )
