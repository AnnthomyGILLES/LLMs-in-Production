import json
import logging

from kafka import KafkaProducer


class KafkaMessageProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info(f"Kafka producer initialized for topic: {self.topic}")

    def send_message(self, message):
        future = self.producer.send(self.topic, message)
        try:
            record_metadata = future.get(timeout=10)
            logging.info(
                f"Message sent successfully to {record_metadata.topic} [{record_metadata.partition}] @ {record_metadata.offset}")
            self.producer.flush()
            logging.info("Producer flushed successfully")
        except Exception as e:
            logging.error(f"Failed to send message: {str(e)}", exc_info=True)

    def close(self):
        self.producer.flush()
        self.producer.close()
        logging.info("Kafka producer closed")


if __name__ == "__main__":
    producer = KafkaMessageProducer(['localhost:9093'], 'your_topic_name')
    try:
        message = {"key": "value"}
        producer.send_message(message)
    finally:
        producer.close()
