import asyncio
import websockets
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

async def log_stream(websocket, path):
    print("Client connected")
    while True:
        log = redis_client.blpop("suricata_logs", timeout=0)
        if log:
            await websocket.send(log[1].decode('utf-8'))

async def main():
    async with websockets.serve(log_stream, "192.168.10.22", 8765):
        print("WebSocket Server Running...")
        await asyncio.Future()

asyncio.run(main())
