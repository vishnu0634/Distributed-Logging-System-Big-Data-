import json
import time
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def register_service():
    registration = {
        "node_id": str(uuid.uuid4()),
        "message_type": "REGISTRATION",
        "service_name": "Microservice3",
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send('service_logs', registration)
    producer.flush()
    return registration["node_id"]

def send_heartbeat(node_id):
    heartbeat = {
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status": "UP",
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send('heartbeats', heartbeat)
    producer.flush()

def log_event(node_id, level, message):
    log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": level,
        "message_type": "LOG",
        "message": message,
        "service_name": "Microservice3",
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send('service_logs', log)
    producer.flush()

if __name__ == '__main__':
    node_id = register_service()
    try:
        while True:
            send_heartbeat(node_id)
            time.sleep(5)

            level = random.choice(["INFO", "WARN", "ERROR", "FATAL"])
            message = f"Sample log message at level {level}"
            log_event(node_id, level, message)
            time.sleep(random.randint(1, 5))

    except KeyboardInterrupt:
        print("Service shutting down.")

