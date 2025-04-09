import asyncio
import websockets
import redis
import os

# === Config ===
LOG_FILE = "/var/log/suricata/suricata_em042714/eve.json"    # Update this!
REDIS_QUEUE = "suricata_logs"
WS_PORT = 8765

# === Connect to Redis ===
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# === Producer: Read logs and push to Redis ===
async def produce_logs():
    print("[Producer] Watching log file...")
    with open(LOG_FILE, "r") as file:
        file.seek(0, os.SEEK_END)
        while True:
            line = file.readline()
            if line:
                redis_client.rpush(REDIS_QUEUE, line.strip())
            else:
                await asyncio.sleep(0.01)

# === WebSocket server handler ===
async def stream_logs(websocket, path):  # ✅ must have (websocket, path)
    print("[Server] Client connected.")
    try:
        while True:
            log = redis_client.blpop(REDIS_QUEUE, timeout=0)
            if log:
                await websocket.send(log[1].decode('utf-8'))
    except websockets.ConnectionClosed:
        print("[Server] Client disconnected.")
    except Exception as e:
        print(f"[Server] Error: {e}")

# === Main async runner ===
async def main():
    print(f"[System] Starting WebSocket server on port {WS_PORT}...")
    ws_server = await websockets.serve(stream_logs, "192.168.10.22", WS_PORT)
    await asyncio.gather(produce_logs(), ws_server.wait_closed())

# === Run ===
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")
