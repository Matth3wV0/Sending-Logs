import asyncio
import websockets
import redis
import os
import time

LOG_FILE = "/var/log/suricata/suricata_em042714"
WS_PORT = 8765
REDIS_QUEUE = "suricata_logs"

redis_client = redis.Redis(host='localhost', port=6379, db=0)

async def produce_logs():
    """ Continuously pushes new log lines to Redis queue """
    with open(LOG_FILE, "r") as file:
        file.seek(0, os.SEEK_END)
        while True:
            line = file.readline()
            if line:
                redis_client.rpush(REDIS_QUEUE, line.strip())
            else:
                await asyncio.sleep(0.01)  # non-blocking sleep

async def stream_logs(websocket, path):
    """ Continuously streams logs from Redis queue to WebSocket client """
    print("WebSocket Client connected")
    try:
        while True:
            log = redis_client.blpop(REDIS_QUEUE, timeout=0)
            if log:
                await websocket.send(log[1].decode('utf-8'))
    except websockets.ConnectionClosed:
        print("Client disconnected")

async def main():
    producer_task = asyncio.create_task(produce_logs())
    websocket_server = websockets.serve(stream_logs, "192.168.10.22", WS_PORT)

    print(f"WebSocket server running on ws://192.168.10.22:{WS_PORT}")
    await asyncio.gather(producer_task, websocket_server)

if __name__ == "__main__":
    asyncio.run(main())
