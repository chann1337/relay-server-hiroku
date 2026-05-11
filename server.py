"""
Hiroku Relay Server - WebSocket relay for bypassing Symmetric NAT
All data goes through WebSocket (single port)
Works on Render Free Tier
"""
import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set
import websockets

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Features supported by this server (v2 protocol)
SERVER_FEATURES: Set[str] = {"zlib", "bin-ping"}

# Pattern for valid feature names (must match on client side too)
_FEATURE_PATTERN = re.compile(r'^[a-z0-9-]{1,32}$')

@dataclass
class Room:
    code: str
    host_name: str
    host_ws: Optional[websockets.WebSocketServerProtocol] = None
    guests: Dict[str, websockets.WebSocketServerProtocol] = field(default_factory=dict)
    lan_motd: str = "Minecraft Server"
    lan_port: int = 25565
    last_activity: float = field(default_factory=time.time)

class RoomManager:
    ROOM_TTL_SECONDS: int = 300

    def __init__(self):
        self.rooms: Dict[str, Room] = {}

    def create_room(self, code: str, host_name: str, host_ws) -> Optional[Room]:
        if code in self.rooms:
            return None
        room = Room(code=code, host_name=host_name, host_ws=host_ws)
        self.rooms[code] = room
        logger.info(f"Room created: {code} by {host_name}")
        return room

    def touch_room(self, code: str) -> None:
        room = self.rooms.get(code)
        if room:
            room.last_activity = time.time()

    async def force_create_room(self, code: str, host_name: str, host_ws) -> tuple[bool, str]:
        room = self.rooms.get(code)
        if room is None:
            new_room = self.create_room(code, host_name, host_ws)
            if new_room:
                return (True, "created")
            return (False, "Room already exists")

        try:
            pong_waiter = room.host_ws.ping()
            await asyncio.wait_for(pong_waiter, timeout=5.0)
            return (False, "Room is actively hosted by another player")
        except (asyncio.TimeoutError, Exception):
            old_host_name = room.host_name
            await self.remove_room(code)
            self.create_room(code, host_name, host_ws)
            logger.warning(f"Force-create: room={code}, old_host={old_host_name}, new_host={host_name}")
            return (True, "created")

    def get_room(self, code: str) -> Optional[Room]:
        return self.rooms.get(code)

    async def remove_room(self, code: str):
        if code in self.rooms:
            room = self.rooms[code]
            for guest_name, guest_ws in room.guests.items():
                try:
                    await guest_ws.send(json.dumps({"action": "room_closed"}))
                except:
                    pass
            del self.rooms[code]
            logger.info(f"Room removed: {code}")

    def add_guest(self, room_code: str, guest_name: str, guest_ws) -> bool:
        room = self.rooms.get(room_code)
        if not room or not room.host_ws:
            return False
        room.guests[guest_name] = guest_ws
        logger.info(f"Guest {guest_name} joined room {room_code}")
        return True

    def remove_guest(self, room_code: str, guest_name: str):
        room = self.rooms.get(room_code)
        if room and guest_name in room.guests:
            del room.guests[guest_name]
            logger.info(f"Guest {guest_name} left room {room_code}")

    async def cleanup_expired_rooms(self) -> int:
        now = time.time()
        expired = []
        for code, room in self.rooms.items():
            elapsed = now - room.last_activity
            if elapsed > self.ROOM_TTL_SECONDS:
                expired.append((code, elapsed, room.host_name))

        for code, elapsed, host_name in expired:
            room = self.rooms.get(code)
            if room and room.host_ws:
                try:
                    await room.host_ws.close()
                except Exception:
                    pass
            await self.remove_room(code)
            logger.info(f"TTL expired: room={code}, elapsed={elapsed:.0f}s, host={host_name}")

        return len(expired)

    def list_rooms(self) -> list:
        return [
            {"code": code, "host_name": room.host_name, "players": 1 + len(room.guests), "max_players": 10}
            for code, room in self.rooms.items()
            if room.host_ws
        ]

room_manager = RoomManager()

async def periodic_cleanup():
    while True:
        await asyncio.sleep(60)
        removed = await room_manager.cleanup_expired_rooms()
        if removed > 0:
            logger.info(f"TTL cleanup: removed {removed} expired rooms")

async def handle_client(websocket):
    client_type = None
    room_code = None
    client_name = None
    room = None
    # v2 handshake state: set to True once a valid "hello" is received
    is_v2: bool = False
    negotiated_features: Set[str] = set()

    try:
        async for message in websocket:
            if client_type == "host" and room_code:
                room_manager.touch_room(room_code)

            if isinstance(message, bytes):
                if is_v2:
                    # v2 opcode-aware dispatch (Requirements 7.4, 8.3, 8.4, 11.1, 12.4, 13.4, 14.5)
                    if not message:
                        continue
                    op = message[0]
                    if op == 0xFE:
                        # Ping: reply with pong on same socket, do not forward
                        logger.debug(f"[relay] binary ping from {websocket.remote_address}, sending pong")
                        try:
                            await websocket.send(bytes([0xFF]))
                        except Exception:
                            pass
                        continue
                    elif op == 0xFF:
                        # Pong: drop, do not forward
                        continue
                    elif op == 0x00 or op == 0x01:
                        # Game data (raw or zlib): forward entire frame verbatim to peers
                        if room and client_type == "host" and room.guests:
                            for guest_ws in list(room.guests.values()):
                                try:
                                    await guest_ws.send(message)
                                except Exception:
                                    pass
                        elif room and client_type == "guest" and room.host_ws:
                            try:
                                await room.host_ws.send(message)
                            except Exception:
                                pass
                    else:
                        # Unrecognized opcode: drop, log, close connection
                        logger.debug(f"[relay] unrecognized opcode 0x{op:02x} from {websocket.remote_address}, closing")
                        try:
                            await websocket.close(1003, "unrecognized opcode")
                        except Exception:
                            pass
                        return
                else:
                    # Legacy v1 connection: forward entire binary message verbatim without inspection
                    if room and client_type == "host" and room.guests:
                        for guest_ws in list(room.guests.values()):
                            try:
                                await guest_ws.send(message)
                            except Exception:
                                pass
                    elif room and client_type == "guest" and room.host_ws:
                        try:
                            await room.host_ws.send(message)
                        except Exception:
                            pass
                continue

            try:
                data = json.loads(message)
                action = data.get("action")

                if action == "hello":
                    # Capability handshake (Requirement 4.2, 4.3, 14.4)
                    raw_features = data.get("features", [])
                    if not isinstance(raw_features, list):
                        raw_features = []
                    # Filter: only accept entries matching ^[a-z0-9-]{1,32}$
                    valid_client_features = {
                        f for f in raw_features
                        if isinstance(f, str) and _FEATURE_PATTERN.match(f)
                    }
                    # Intersect with server-supported features
                    negotiated_features = valid_client_features & SERVER_FEATURES
                    is_v2 = True
                    remote_addr = websocket.remote_address
                    await websocket.send(json.dumps({
                        "action": "welcome",
                        "version": 2,
                        "features": sorted(negotiated_features),
                    }))
                    logger.info(
                        f"[relay] welcome sent to {remote_addr}: features={list(negotiated_features)}"
                    )

                elif action == "create":
                    room_code = data["code"]
                    client_name = data.get("host_name", "Host")
                    force = data.get("force", False)
                    if force:
                        success, msg = await room_manager.force_create_room(room_code, client_name, websocket)
                        if success:
                            client_type = "host"
                            room = room_manager.get_room(room_code)
                            await websocket.send(json.dumps({"action": "created", "code": room_code}))
                        else:
                            await websocket.send(json.dumps({"action": "error", "message": msg}))
                    else:
                        room = room_manager.create_room(room_code, client_name, websocket)
                        if room:
                            client_type = "host"
                            await websocket.send(json.dumps({"action": "created", "code": room_code}))
                            logger.info(f"Room {room_code} created")
                        else:
                            await websocket.send(json.dumps({"action": "error", "message": "Room already exists"}))

                elif action == "join":
                    room_code = data["code"]
                    client_name = data.get("guest_name", "Guest")
                    client_type = "guest"
                    room = room_manager.get_room(room_code)
                    if room and room.host_ws:
                        if room_manager.add_guest(room_code, client_name, websocket):
                            await room.host_ws.send(json.dumps({
                                "action": "guest_joined",
                                "guest_name": client_name,
                                "total_players": 1 + len(room.guests)
                            }))
                            await websocket.send(json.dumps({
                                "action": "joined",
                                "code": room_code,
                                "host_name": room.host_name,
                                "players": 1 + len(room.guests)
                            }))
                            await websocket.send(json.dumps({
                                "action": "lan_info",
                                "motd": room.lan_motd,
                                "port": room.lan_port
                            }))
                        else:
                            await websocket.send(json.dumps({"action": "error", "message": "Failed to join room"}))
                    else:
                        await websocket.send(json.dumps({"action": "error", "message": "Room not found"}))

                elif action == "list":
                    await websocket.send(json.dumps({"action": "room_list", "rooms": room_manager.list_rooms()}))

                elif action == "lan_info":
                    if room and client_type == "host":
                        room.lan_motd = data.get("motd", "Minecraft Server")
                        room.lan_port = data.get("port", 25565)
                        for guest_ws in list(room.guests.values()):
                            try:
                                await guest_ws.send(json.dumps({
                                    "action": "lan_info",
                                    "motd": room.lan_motd,
                                    "port": room.lan_port
                                }))
                            except:
                                pass

                elif action == "close":
                    if room_code and client_type == "host":
                        await room_manager.remove_room(room_code)
                        await websocket.send(json.dumps({"action": "closed"}))

                elif action == "leave":
                    if room_code and client_type == "guest" and client_name:
                        room_manager.remove_guest(room_code, client_name)
                        room = room_manager.get_room(room_code)
                        if room and room.host_ws:
                            try:
                                await room.host_ws.send(json.dumps({
                                    "action": "guest_left",
                                    "guest_name": client_name,
                                    "total_players": 1 + len(room.guests)
                                }))
                            except:
                                pass
                        await websocket.send(json.dumps({"action": "left"}))

                elif action == "ping":
                    await websocket.send(json.dumps({"action": "pong"}))

            except json.JSONDecodeError:
                await websocket.send(json.dumps({"action": "error", "message": "Invalid JSON"}))
            except Exception as e:
                logger.error(f"Error handling message: {e}")

    except websockets.exceptions.ConnectionClosed as exc:
        if exc.rcvd is None or (exc.rcvd and exc.rcvd.code in (1011, 1002, 1006)):
            logger.info(f"Connection closed due to ping timeout: {client_type} {client_name} ({room_code})")
    finally:
        if client_type == "host" and room_code:
            await room_manager.remove_room(room_code)
        elif client_type == "guest" and room_code and client_name:
            room_manager.remove_guest(room_code, client_name)
            room = room_manager.get_room(room_code)
            if room and room.host_ws:
                try:
                    await room.host_ws.send(json.dumps({
                        "action": "guest_left",
                        "guest_name": client_name,
                        "total_players": 1 + len(room.guests)
                    }))
                except:
                    pass
        logger.info(f"Client disconnected: {client_type} {client_name} ({room_code})")

async def main():
    port = int(os.environ.get("PORT", 8765))
    host = "0.0.0.0"
    logger.info(f"Starting Hiroku WebSocket Relay Server on {host}:{port}")
    cleanup_task = asyncio.create_task(periodic_cleanup())
    async with websockets.serve(handle_client, host, port, max_size=1_048_576, origins=None, ping_interval=20, ping_timeout=30):
        logger.info("Server started. Press Ctrl+C to stop.")
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            cleanup_task.cancel()
            logger.info("Shutting down...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped.")
