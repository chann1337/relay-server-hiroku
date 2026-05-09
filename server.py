"""
Hiroku Relay Server - WebSocket relay для обхода Symmetric NAT
ВСЕ данные идут через WebSocket (один порт)
Работает на Render Free Tier
"""
import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, Optional, List
import websockets
from websockets.frames import Frame

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Room:
    code: str
    host_name: str
    host_ws: Optional[websockets.WebSocketServerProtocol] = None
    host_ws_queue: Optional[asyncio.Queue] = None
    guests: Dict[str, websockets.WebSocketServerProtocol] = field(default_factory=dict)
    guest_ws_queues: Dict[str, asyncio.Queue] = field(default_factory=dict)

class RoomManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}

    def create_room(self, code: str, host_name: str, host_ws) -> Optional[Room]:
        if code in self.rooms:
            return None
        room = Room(
            code=code, 
            host_name=host_name, 
            host_ws=host_ws,
            host_ws_queue=asyncio.Queue()
        )
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
            room.guest_ws_queues.clear()
            if room.host_ws_queue:
                while not room.host_ws_queue.empty():
                    try:
                        room.host_ws_queue.get_nowait()
                    except:
                        break
            del self.rooms[code]
            logger.info(f"Room removed: {code}")

    def add_guest(self, room_code: str, guest_name: str, guest_ws) -> bool:
        room = self.rooms.get(room_code)
        if not room or not room.host_ws:
            return False
        room.guests[guest_name] = guest_ws
        room.guest_ws_queues[guest_name] = asyncio.Queue()
        logger.info(f"Guest {guest_name} joined room {room_code}")
        return True

    def remove_guest(self, room_code: str, guest_name: str):
        room = self.rooms.get(room_code)
        if room and guest_name in room.guests:
            del room.guests[guest_name]
            if guest_name in room.guest_ws_queues:
                del room.guest_ws_queues[guest_name]
            logger.info(f"Guest {guest_name} left room {room_code}")

    def list_rooms(self) -> list:
        return [
            {
                "code": code,
                "host_name": room.host_name,
                "players": 1 + len(room.guests),
                "max_players": 10
            }
            for code, room in self.rooms.items()
            if room.host_ws
        ]

room_manager = RoomManager()

async def relay_game_data(ws, queue: asyncio.Queue, is_host: bool, room_code: str, client_name: str):
    """Пересылка бинарных игровых данных через WebSocket"""
    try:
        while True:
            data = await queue.get()
            if data is None:
                break
            await ws.send(data)
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Relay error ({client_name}): {e}")

async def handle_client(websocket, path):
    """Обработка WebSocket соединений"""
    client_type = None
    room_code = None
    client_name = None
    room = None
    tasks = []

    try:
        async for message in websocket:
            if isinstance(message, bytes):
                # Бинарные данные игры
                if room and client_type == "host":
                    for guest_name, guest_queue in room.guest_ws_queues.items():
                        await guest_queue.put(message)
                elif room and client_type == "guest" and room.host_ws_queue:
                    await room.host_ws_queue.put(message)
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
                        await websocket.send(json.dumps({
                            "action": "created",
                            "code": room_code,
                            "server_url": os.environ.get("SERVER_URL", "wss://localhost:8765")
                        }))
                        logger.info(f"Room {room_code} created")
                    else:
                        await websocket.send(json.dumps({
                            "action": "error",
                            "message": "Room already exists"
                        }))

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
                                "server_url": os.environ.get("SERVER_URL", "wss://localhost:8765"),
                                "players": 1 + len(room.guests)
                            }))
                            
                            # Запускаем relay для гостя
                            task = asyncio.create_task(relay_game_data(
                                websocket, room.guest_ws_queues[client_name], False, room_code, client_name
                            ))
                            tasks.append(task)
                        else:
                            await websocket.send(json.dumps({
                                "action": "error",
                                "message": "Failed to join room"
                            }))
                    else:
                        await websocket.send(json.dumps({
                            "action": "error",
                            "message": "Room not found"
                        }))

                elif action == "list":
                    rooms = room_manager.list_rooms()
                    await websocket.send(json.dumps({
                        "action": "room_list",
                        "rooms": rooms
                    }))

                elif action == "close":
                    if room_code and client_type == "host":
                        await room_manager.remove_room(room_code)
                        await websocket.send(json.dumps({"action": "closed"}))

                elif action == "leave":
                    if room_code and client_type == "guest" and client_name:
                        room_manager.remove_guest(room_code, client_name)
                        room = room_manager.get_room(room_code)
                        if room and room.host_ws:
                            await room.host_ws.send(json.dumps({
                                "action": "guest_left",
                                "guest_name": client_name,
                                "total_players": 1 + len(room.guests)
                            }))
                        await websocket.send(json.dumps({"action": "left"}))

                elif action == "ping":
                    await websocket.send(json.dumps({"action": "pong"}))

            except json.JSONDecodeError:
                await websocket.send(json.dumps({"action": "error", "message": "Invalid JSON"}))
            except Exception as e:
                logger.error(f"Error handling message: {e}")
                await websocket.send(json.dumps({"action": "error", "message": str(e)}))

    except asyncio.CancelledError:
        pass
    finally:
        for task in tasks:
            task.cancel()
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
    logger.info("All game traffic goes through WebSocket (single port)")
    logger.info("Works on Render Free Tier")

    async with websockets.serve(handle_client, host, port):
        logger.info("WebSocket server started. Press Ctrl+C to stop.")
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            logger.info("Shutting down...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped.")
