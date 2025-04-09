import asyncio
import websockets
import json

WS_SERVER = "ws://192.168.10.22:8765"
OUTPUT_FILE = "/home/ubuntu/Desktop/suricata.json"

async def receive_logs():
    async with websockets.connect(WS_SERVER) as websocket:
        print("Connected...")
        async for log_entry in websocket:
            log_json = json.loads(log_entry)
            # Process log entry (ML model or file)
            with open(OUTPUT_FILE, "a") as f:
                json.dump(log_json, f)
                f.write("\n")

asyncio.run(receive_logs())
