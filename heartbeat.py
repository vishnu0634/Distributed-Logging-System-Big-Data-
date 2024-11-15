import json
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

def send_heartbeat(producer, node_id, service_name, interval=5):
    """
    Sends a heartbeat message to Kafka at regular intervals.
    
    Parameters:
    - producer: KafkaProducer object to send messages.
    - node_id: Unique identifier of the microservice node.
    - service_name: Name of the microservice.
    - interval: Time interval (in seconds) between heartbeats.
    """
    topic = 'heartbeats'
    while True:
        heartbeat_message = {
            "node_id": node_id,
            "message_type": "HEARTBEAT",
            "status": "UP",
            "service_name": service_name,
            "timestamp": datetime.utcnow().isoformat()
        }
        producer.send(topic, value=heartbeat_message)
        print(f"Sent heartbeat: {heartbeat_message}")
        time.sleep(interval)

if __name__ == '__main__':
    # Unique ID for the node
    node_id = str(uuid.uuid4())
    # Service name (you can customize this for each microservice)
    service_name = "Microservice1"  # Change to "Microservice2" or "Microservice3" as needed

    # Kafka Producer setup
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Start sending heartbeat messages
    send_heartbeat(producer, node_id, service_name, interval=5)

