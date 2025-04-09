import redis
import os
import time

LOG_FILE = "/var/log/suricata/suricata_em042714"
redis_client = redis.Redis(host='localhost', port=6379, db=0)

with open(LOG_FILE, "r") as file:
    file.seek(0, os.SEEK_END)
    while True:
        line = file.readline()
        if line:
            redis_client.rpush("suricata_logs", line.strip())
        else:
            time.sleep(0.01)
