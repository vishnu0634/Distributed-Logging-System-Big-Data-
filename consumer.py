import json
from kafka import KafkaConsumer

# Configure the Kafka consumer to listen to service_logs and heartbeats topics
consumer_logs = KafkaConsumer(
    'service_logs',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

consumer_heartbeats = KafkaConsumer(
    'heartbeats',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening to service logs and heartbeat messages...")

try:
    while True:
        # Poll for messages
        for message in consumer_logs:
            print("Log Message:", message.value)

        for message in consumer_heartbeats:
            print("Heartbeat Message:", message.value)

except KeyboardInterrupt:
    print("Consumer stopped.")
