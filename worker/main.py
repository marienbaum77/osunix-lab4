from kafka import KafkaConsumer
import json
import socket
import time
import os

worker_id = os.uname().nodename
RESULTS_FILE = "/data/scan_log.txt"

def scan_ports(target, ports):
    open_ports = []
    print(f"[{worker_id}] Scanning {target}: ports {ports[0]}-{ports[-1]}...")
    
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5)
        result = sock.connect_ex((target, port))
        if result == 0:
            open_ports.append(port)
        sock.close()
    
    return open_ports

def save_results(target, open_ports):
    if not open_ports:
        return
    
    with open(RESULTS_FILE, "a") as f:
        for port in open_ports:
            line = f"[{worker_id}] Found OPEN port on {target}: {port}\n"
            print(line.strip())
            f.write(line)

def start_worker():
    print(f"Scanner Worker {worker_id} started.")
    consumer = None
    while not consumer:
        try:
            consumer = KafkaConsumer(
                'scan_tasks',
                bootstrap_servers=['kafka:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='scanner_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except:
            time.sleep(5)

    for message in consumer:
        task = message.value
        target = task['target']
        ports = task['ports']

        found = scan_ports(target, ports)
        save_results(target, found)

if __name__ == "__main__":
    start_worker()