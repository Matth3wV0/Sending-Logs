#!/usr/bin/env python3
import redis
import json
import time
import os
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class SuricataLogHandler(FileSystemEventHandler):
    def __init__(self, redis_client, channel, logfile_path):
        self.redis_client = redis_client
        self.channel = channel
        self.logfile_path = logfile_path
        self.last_position = 0
        self.initialize_position()
        
    def initialize_position(self):
        """Initialize the last read position to the end of the file"""
        if os.path.exists(self.logfile_path):
            with open(self.logfile_path, 'r') as f:
                f.seek(0, os.SEEK_END)
                self.last_position = f.tell()
    
    def on_modified(self, event):
        if event.src_path == self.logfile_path:
            self.process_new_logs()
            
    def process_new_logs(self):
        """Read new logs from file and publish to Redis"""
        try:
            with open(self.logfile_path, 'r') as f:
                f.seek(self.last_position)
                new_data = f.read()
                if new_data:
                    # Split by newlines in case there are multiple JSON objects
                    for line in new_data.strip().split('\n'):
                        if line:  # Skip empty lines
                            try:
                                # Validate JSON before publishing
                                json_data = json.loads(line)
                                # Add timestamp for tracking
                                json_data['_redis_publish_time'] = time.time()
                                # Publish to Redis
                                self.redis_client.publish(
                                    self.channel, 
                                    json.dumps(json_data)
                                )
                                print(f"Published log entry: {json_data.get('event_type', 'unknown')}")
                            except json.JSONDecodeError:
                                print(f"Skipping invalid JSON: {line[:100]}...")
                                
                self.last_position = f.tell()
        except Exception as e:
            print(f"Error processing logs: {e}")
            
    def manual_check(self):
        """Manually check for new logs (used for polling)"""
        self.process_new_logs()

def main():
    parser = argparse.ArgumentParser(description='Suricata JSON log publisher for Redis')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-password', default=None, help='Redis password')
    parser.add_argument('--channel', default='suricata-logs', help='Redis channel name')
    parser.add_argument('--logfile', required=True, help='Path to suricata.json file')
    parser.add_argument('--poll-interval', type=float, default=0.1, 
                      help='Polling interval in seconds (use with --poll)')
    parser.add_argument('--poll', action='store_true', 
                      help='Use polling instead of file system events')
    
    args = parser.parse_args()
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_password,
        decode_responses=True
    )
    
    # Test Redis connection
    try:
        redis_client.ping()
        print(f"Connected to Redis at {args.redis_host}:{args.redis_port}")
    except redis.ConnectionError as e:
        print(f"Failed to connect to Redis: {e}")
        return

    # Create event handler
    event_handler = SuricataLogHandler(redis_client, args.channel, args.logfile)
    
    if args.poll:
        # Use polling approach (more reliable in some environments)
        print(f"Starting polling for changes to {args.logfile} every {args.poll_interval} seconds")
        try:
            while True:
                event_handler.manual_check()
                time.sleep(args.poll_interval)
        except KeyboardInterrupt:
            print("Stopping polling")
    else:
        # Use file system events
        observer = Observer()
        observer.schedule(event_handler, path=os.path.dirname(args.logfile), recursive=False)
        print(f"Starting file system observer for {args.logfile}")
        
        try:
            observer.start()
            # Keep the main thread running
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
            print("Observer stopped")
        observer.join()

if __name__ == "__main__":
    main()