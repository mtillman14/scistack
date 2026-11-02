"""The /ws route. Connection handling and fan-out live in ``ws_hub``."""

from fastapi import APIRouter, WebSocket

from scistack_gui.runtime import ws_hub

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await ws_hub.serve(websocket)
