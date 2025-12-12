from fastapi import FastAPI
from kafka import KafkaProducer
from pydantic import BaseModel
import json
import time

app = FastAPI()

class ScanRequest(BaseModel):
    target: str
    start_port: int
    end_port: int  

def get_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print("Waiting for Kafka", e)
            time.sleep(5)

producer = None

@app.on_event("startup")
def startup_event():
    global producer
    producer = get_producer()

@app.post("/scan")
async def start_scan(request: ScanRequest):
    BATCH_SIZE = 20
    tasks_count = 0
    for i in range(request.start_port, request.end_port + 1, BATCH_SIZE):
        batch_end = min(i + BATCH_SIZE - 1, request.end_port)
        ports_batch = list(range(i, batch_end + 1))
        
        task_payload = {
            "target": request.target,
            "ports": ports_batch
        }
        
        producer.send('scan_tasks', task_payload)
        tasks_count += 1

    return {
        "status": "Scan started", 
        "target": request.target, 
        "batches_sent": tasks_count,
        "message": "Tasks distributed to workers"
    }