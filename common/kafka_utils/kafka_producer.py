import logging
from typing import Any, Dict

from kafka import KafkaProducer
from kafka.errors import KafkaError

from common.kafka_utils.serializers import serialize_message

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class KafkaProducerWrapper:
    """A Kafka producer for sending messages to a specified topic."""

    def __init__(self, bootstrap_servers: list, topic: str, **kwargs):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: serialize_message(v),
            **kwargs
        )
        logging.info(f"Kafka producer initialized for topic: {self.topic}")

    def send_message(self, message: Dict[str, Any], retries: int = 3) -> bool:
        for attempt in range(retries):
            try:
                self.producer.send(self.topic, message)
                self.producer.flush()
                logging.info("Producer flushed successfully")
                return True
            except KafkaError as e:
                logging.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == retries - 1:
                    logging.error(f"Failed to send message after {retries} attempts", exc_info=True)
                    return False
        return False

    def close(self):
        """Close the Kafka producer."""
        self.producer.flush()
        self.producer.close()
        logging.info("Kafka producer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    with KafkaProducerWrapper(['localhost:9092'], 'your_topic_name') as producer:
        message = {"key": "value"}
        success = producer.send_message(message)
        if success:
            print("Message sent successfully")
        else:
            print("Failed to send message")
