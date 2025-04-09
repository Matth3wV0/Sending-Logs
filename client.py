import asyncio
import websockets
import json

WS_SERVER = "ws://192.168.10.22:8765"   # Replace with actual IP!
OUTPUT_FILE = "/home/ubuntu/Desktop/suricata.json"

async def receive_logs():
    try:
        async with websockets.connect(WS_SERVER) as websocket:
            print(f"[Client] Connected to {WS_SERVER}")
            async for log_entry in websocket:
                try:
                    log_json = json.loads(log_entry)
                    print("[Client] Received log:", log_json)

                    # Save to file (or pass to ML model here)
                    with open(OUTPUT_FILE, "a") as f:
                        json.dump(log_json, f)
                        f.write("\n")

                except json.JSONDecodeError as e:
                    print(f"[Client] Invalid JSON: {log_entry}")
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"[Client] WebSocket closed unexpectedly: {e}")
    except Exception as e:
        print(f"[Client] General error: {e}")

# Run the client
if __name__ == "__main__":
    try:
        asyncio.run(receive_logs())
    except KeyboardInterrupt:
        print("[Client] Stopped by user")
