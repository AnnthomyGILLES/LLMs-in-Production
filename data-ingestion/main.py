import logging
import os

from kafka_consumer import KafkaConsumerWrapper
from mongodb_writer import MongoDBWriter


def main():
    logging.basicConfig(level=logging.INFO)

    kafka_servers = os.getenv('KAFKA_SERVERS', 'localhost:9093').split(',')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'new-incoming-data')
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    mongo_db = os.getenv('MONGO_DB', 'llmtoprod_db')
    mongo_collection = os.getenv('MONGO_COLLECTION', 'kafka_messages')

    consumer = KafkaConsumerWrapper(kafka_servers, kafka_topic)
    writer = MongoDBWriter(mongo_uri, mongo_db, mongo_collection)

    try:
        for message in consumer.consume():
            writer.write(message)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        consumer.close()
        writer.client.close()


if __name__ == "__main__":
    main()
