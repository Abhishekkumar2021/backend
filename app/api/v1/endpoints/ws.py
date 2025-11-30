import asyncio
import json

import redis.asyncio as redis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.config import settings

router = APIRouter()


# Global list to hold active WebSocket connections for broadcasting
# In a real app, you might want a more sophisticated connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[int, list[WebSocket]] = {}  # {job_id: [websocket_client]}

    async def connect(self, websocket: WebSocket, job_id: int):
        await websocket.accept()
        if job_id not in self.active_connections:
            self.active_connections[job_id] = []
        self.active_connections[job_id].append(websocket)

    def disconnect(self, websocket: WebSocket, job_id: int):
        if job_id in self.active_connections:
            self.active_connections[job_id].remove(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str, job_id: int):
        if job_id in self.active_connections:
            for connection in self.active_connections[job_id]:
                await connection.send_text(message)


manager = ConnectionManager()


@router.websocket("/ws/jobs/{job_id}/status")
async def websocket_endpoint(websocket: WebSocket, job_id: int):
    await manager.connect(websocket, job_id)
    r = redis.from_url(settings.REDIS_URL, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(f"job_updates:{job_id}")
    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                await manager.send_personal_message(message["data"], websocket)
            # Keep the connection alive
            await websocket.send_text(json.dumps({"type": "ping"}))  # send a ping to keep connection open if no data
            await asyncio.sleep(5)  # Ping every 5 seconds
    except WebSocketDisconnect:
        manager.disconnect(websocket, job_id)
        await pubsub.unsubscribe(f"job_updates:{job_id}")
        await pubsub.close()
    except asyncio.CancelledError:
        manager.disconnect(websocket, job_id)
        await pubsub.unsubscribe(f"job_updates:{job_id}")
        await pubsub.close()
    finally:
        pass
