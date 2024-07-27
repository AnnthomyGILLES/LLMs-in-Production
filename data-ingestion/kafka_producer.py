import json
import logging
import random
import time

from kafka import KafkaProducer


class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info(f"Kafka producer initialized. Servers: {bootstrap_servers}")

    def produce_messages(self, topic, num_messages=10, delay=1):
        """
        Produces a specified number of messages to a Kafka topic.

        :param topic: Kafka topic to produce to
        :param num_messages: Number of messages to produce
        :param delay: Delay between messages in seconds
        """
        for i in range(num_messages):
            message = self.generate_message(i)
            self.producer.send(topic, value=message)
            logging.info(f"Produced message {i + 1}: {message}")
            time.sleep(delay)

        self.producer.flush()
        logging.info(f"Finished producing {num_messages} messages to topic: {topic}")

    def generate_message(self, index):
        """
        Generates a sample message. Modify this method to create the type of data you need.
        """
        return {
            "id": index,
            "timestamp": time.time(),
            "value": random.uniform(0, 100),
            "status": random.choice(["active", "inactive", "pending"])
        }

    def close(self):
        self.producer.close()
        logging.info("Kafka producer closed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = KafkaProducerWrapper(['localhost:9093'])
    producer.produce_messages('incoming-data', num_messages=5, delay=2)
    producer.close()
