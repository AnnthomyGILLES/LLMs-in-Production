import time
import uuid

from faker import Faker
from kafka import KafkaProducer

from kafka_stream.utils import json_serializer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=json_serializer
)

if __name__ == '__main__':
    while True:
        message = {
            "name": fake.word(),
            "link": fake.url(),
            "content": {"description": fake.text()},
            "owner_id": str(uuid.uuid4()),
        }

        future = producer.send('incoming-data', message)
        result = future.get(timeout=60)

        print(f"Message sent: {message}")
        print(f"Partition: {result.partition}")
        print(f"Offset: {result.offset}")

        time.sleep(5)
