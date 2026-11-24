"""Lightweight 2-person WebSocket voice relay server for Render.com (Free Tier)."""

import asyncio
from http import HTTPStatus
import json
import os
from typing import Any, Optional

import websockets
from websockets.asyncio.server import ServerConnection, serve
from websockets.datastructures import Headers
from websockets.http11 import Response

PORT = int(os.environ.get("PORT", "10000"))
ROOM_PASSWORD = os.environ.get("ROOM_PASSWORD", "").strip()

# Active clients: ws -> {"nick": str, "state": dict}
CLIENTS: dict[ServerConnection, dict[str, Any]] = {}


def health_check(connection: ServerConnection, request) -> Optional[Response]:
    """Respond 200 OK to plain HTTP GET health checks (required by Render.com)."""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return Response(
            HTTPStatus.OK,
            "OK",
            Headers([("Content-Type", "text/plain; charset=utf-8")]),
            b"TeamCzat Relay OK (max 2 users)\n",
        )
    return None


def _other_client(ws: ServerConnection) -> Optional[ServerConnection]:
    for peer in CLIENTS:
        if peer is not ws:
            return peer
    return None


async def _send_json(ws: ServerConnection, payload: dict[str, Any]) -> None:
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


async def handler(ws: ServerConnection) -> None:
    joined = False
    try:
        # First message must be JOIN
        raw = await asyncio.wait_for(ws.recv(), timeout=15.0)
        if not isinstance(raw, str):
            await ws.close(1008, "Expected JOIN JSON")
            return

        try:
            msg = json.loads(raw)
        except Exception:
            await ws.close(1008, "Invalid JSON")
            return

        if msg.get("type") != "JOIN":
            await ws.close(1008, "Expected JOIN")
            return

        pw = str(msg.get("password", "")).strip()
        if ROOM_PASSWORD and pw != ROOM_PASSWORD:
            await _send_json(ws, {"type": "REJECT", "reason": "Nieprawidłowe hasło pokoju."})
            await ws.close(1008, "Bad password")
            return

        if len(CLIENTS) >= 2:
            await _send_json(ws, {"type": "REJECT", "reason": "Kanał jest pełny (2/2) — tylko dla dwóch osób!"})
            await ws.close(1008, "Room full")
            return

        nick = str(msg.get("nick") or "Użytkownik")[:24]
        state = msg.get("state") or {}
        client_info = {
            "nick": nick,
            "mic_muted": bool(state.get("mic_muted", False)),
            "deafened": bool(state.get("deafened", False)),
            "speaking": False,
        }

        existing_peer = _other_client(ws)
        CLIENTS[ws] = client_info
        joined = True

        # Send WELCOME with existing peer info (if any)
        peer_info = CLIENTS[existing_peer] if existing_peer is not None else None
        await _send_json(ws, {"type": "WELCOME", "peer": peer_info})

        # Notify existing peer that a second user joined
        if existing_peer is not None:
            await _send_json(existing_peer, {"type": "PEER_JOINED", "peer": client_info})

        # Main message loop
        async for frame in ws:
            peer = _other_client(ws)
            if isinstance(frame, bytes):
                # Raw PCM voice frame -> relay directly to the other user
                if peer is not None:
                    try:
                        await peer.send(frame)
                    except Exception:
                        pass
            else:
                try:
                    data = json.loads(frame)
                except Exception:
                    continue
                mtype = data.get("type")
                if mtype == "PING":
                    await _send_json(ws, {"type": "PONG", "ts": data.get("ts", 0.0)})
                elif mtype == "STATE":
                    client_info["nick"] = str(data.get("nick") or client_info["nick"])[:24]
                    client_info["mic_muted"] = bool(data.get("mic_muted", False))
                    client_info["deafened"] = bool(data.get("deafened", False))
                    client_info["speaking"] = bool(data.get("speaking", False))
                    if peer is not None:
                        await _send_json(peer, {"type": "STATE", **client_info})
                elif mtype == "POKE":
                    if peer is not None:
                        await _send_json(
                            peer,
                            {
                                "type": "POKE",
                                "from": str(data.get("from") or client_info["nick"]),
                                "message": str(data.get("message") or ""),
                            },
                        )

    except (asyncio.TimeoutError, websockets.ConnectionClosed):
        pass
    finally:
        if joined and ws in CLIENTS:
            left_nick = CLIENTS[ws]["nick"]
            del CLIENTS[ws]
            peer = _other_client(ws)
            if peer is not None:
                await _send_json(peer, {"type": "PEER_LEFT", "nick": left_nick, "reason": "rozłączył się"})


async def main() -> None:
    print(f"TeamCzat Relay starting on 0.0.0.0:{PORT} (password={'SET' if ROOM_PASSWORD else 'NONE'})", flush=True)
    async with serve(
        handler,
        "0.0.0.0",
        PORT,
        process_request=health_check,
        ping_interval=20,
        ping_timeout=20,
        max_size=65536,
    ):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
