"""
Hiroku Relay Server - WebSocket relay for bypassing Symmetric NAT
All data goes through WebSocket (single port)
Works on Render Free Tier
"""
import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, Optional
import websockets

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Room:
    code: str
    host_name: str
    host_ws: Optional[websockets.WebSocketServerProtocol] = None
    guests: Dict[str, websockets.WebSocketServerProtocol] = field(default_factory=dict)
    lan_motd: str = "Minecraft Server"
    lan_port: int = 25565

class RoomManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}

    def create_room(self, code: str, host_name: str, host_ws) -> Optional[Room]:
        if code in self.rooms:
            return None
        room = Room(code=code, host_name=host_name, host_ws=host_ws)
        self.rooms[code] = room
        logger.info(f"Room created: {code} by {host_name}")
        return room

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

    def list_rooms(self) -> list:
        return [
            {"code": code, "host_name": room.host_name, "players": 1 + len(room.guests), "max_players": 10}
            for code, room in self.rooms.items()
            if room.host_ws
        ]

room_manager = RoomManager()

async def handle_client(websocket):
    client_type = None
    room_code = None
    client_name = None
    room = None

    try:
        async for message in websocket:
            if isinstance(message, bytes):
                if room and client_type == "host" and room.guests:
                    for guest_ws in list(room.guests.values()):
                        try:
                            await guest_ws.send(message)
                        except:
                            pass
                elif room and client_type == "guest" and room.host_ws:
                    try:
                        await room.host_ws.send(message)
                    except:
                        pass
                continue

            try:
                data = json.loads(message)
                action = data.get("action")

                if action == "create":
                    room_code = data["code"]
                    client_name = data.get("host_name", "Host")
                    client_type = "host"
                    room = room_manager.create_room(room_code, client_name, websocket)
                    if room:
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

    except websockets.exceptions.ConnectionClosed:
        pass
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
    async with websockets.serve(handle_client, host, port):
        logger.info("Server started. Press Ctrl+C to stop.")
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Shutting down...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped.")
