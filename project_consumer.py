from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'fluentd-consumer-group',   # Consumer group id
    'auto.offset.reset': 'earliest'         # Start from the earliest message
}

consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe(['fluentd_logs'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("No message received")  # Debug print
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")  # Detailed error logging
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition {msg.partition}")
        else:
            try:
                decoded_message = msg.value().decode('utf-8')
                print(f"Raw message: {decoded_message}")
                # Optional: Parse JSON if needed
                parsed_message = json.loads(decoded_message)
                print(f"Parsed message: {parsed_message}")
            except Exception as e:
                print(f"Message processing error: {e}")
                print(f"Raw message bytes: {msg.value()}")
except KeyboardInterrupt:
    print("Consumer interrupted")
finally:
    consumer.close()

