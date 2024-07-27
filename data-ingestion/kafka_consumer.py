import json
import logging

from kafka import KafkaConsumer


class KafkaConsumerWrapper:
    def __init__(self, bootstrap_servers=None, topic='new-incoming-data', group_id='my-group'):
        if bootstrap_servers is None:
            bootstrap_servers = ['localhost:9093']
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"Kafka consumer initialized. Topic: {topic}")

    def consume(self):
        logging.info("Starting to consume messages...")
        for message in self.consumer:
            logging.info(f"Received message: {message.value}")
            logging.info(f"Partition: {message.partition}")
            logging.info(f"Offset: {message.offset}")

            document = {
                'value': message.value,
                'topic': message.topic,
                'partition': message.partition,
                'offset': message.offset,
                'timestamp': message.timestamp
            }

            yield document

    def close(self):
        self.consumer.close()
        logging.info("Kafka consumer closed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = KafkaConsumerWrapper()
    for msg in consumer.consume():
        print(msg)
    consumer.close()
