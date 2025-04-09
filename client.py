#!/usr/bin/env python3
import redis
import json
import time
import os
import argparse
import threading
import queue
from datetime import datetime

class SuricataLogSubscriber:
    def __init__(self, redis_client, channel, output_file):
        self.redis_client = redis_client
        self.channel = channel
        self.output_file = output_file
        self.pubsub = self.redis_client.pubsub()
        self.log_queue = queue.Queue()
        self.running = True
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        
    def start(self):
        """Start the subscriber in a separate thread"""
        # Start the writer thread
        writer_thread = threading.Thread(target=self.log_writer)
        writer_thread.daemon = True
        writer_thread.start()
        
        # Subscribe to Redis channel
        self.pubsub.subscribe(**{self.channel: self.message_handler})
        
        # Start processing messages
        print(f"Listening for messages on channel: {self.channel}")
        try:
            self.pubsub.run_in_thread(sleep_time=0.001)
            
            # Keep main thread alive
            while self.running:
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            print("Stopping subscriber")
            self.running = False
            
    def message_handler(self, message):
        """Handle incoming Redis messages"""
        if message['type'] == 'message':
            try:
                # Add to processing queue
                self.log_queue.put(message['data'])
            except Exception as e:
                print(f"Error handling message: {e}")
                
    def log_writer(self):
        """Writer thread that processes the queue and writes to file"""
        while self.running:
            try:
                # Get data from queue with timeout
                try:
                    data = self.log_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                
                # Write to output file
                with open(self.output_file, 'a') as f:
                    f.write(data + '\n')
                    
                # Mark task as done
                self.log_queue.task_done()
                
                # Log receipt
                try:
                    log_data = json.loads(data)
                    event_type = log_data.get('event_type', 'unknown')
                    timestamp = log_data.get('timestamp', datetime.now().isoformat())
                    print(f"[{timestamp}] Received log entry: {event_type}")
                except json.JSONDecodeError:
                    print(f"Received non-JSON data: {data[:50]}...")
                
            except Exception as e:
                print(f"Error in writer thread: {e}")
                time.sleep(1)  # Avoid tight error loops

def main():
    parser = argparse.ArgumentParser(description='Suricata JSON log subscriber for SIEM')
    parser.add_argument('--redis-host', required=True, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-password', default=None, help='Redis password')
    parser.add_argument('--channel', default='suricata-logs', help='Redis channel name')
    parser.add_argument('--output-file', required=True, 
                      help='Path to output file for logs')
    
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

    # Create and start subscriber
    subscriber = SuricataLogSubscriber(
        redis_client,
        args.channel,
        args.output_file
    )
    
    try:
        subscriber.start()
    except KeyboardInterrupt:
        print("Stopping subscriber")

if __name__ == "__main__":
    main()