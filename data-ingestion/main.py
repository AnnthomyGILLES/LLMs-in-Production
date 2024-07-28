import logging
import os
import threading

from fake_api_streamer import FakeAPIStreamer
from kafka_consumer import KafkaConsumerWrapper
from kafka_producer import KafkaProducerWrapper
from mongodb_writer import MongoDBWriter


def stream_api_data(api_streamer):
    api_streamer.stream_data()


def consume_and_write(consumer, writer):
    for message in consumer.consume():
        writer.write(message)


def main():
    logging.basicConfig(level=logging.INFO)

    kafka_servers = os.getenv('KAFKA_SERVERS', 'kafka:9092').split(',')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'incoming-data')
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
    mongo_db = os.getenv('MONGO_DB', 'llmtoprod_db')
    mongo_collection = os.getenv('MONGO_COLLECTION', 'kafka_messages')

    producer = KafkaProducerWrapper(kafka_servers)
    consumer = KafkaConsumerWrapper(kafka_servers, kafka_topic)
    writer = MongoDBWriter(mongo_uri, mongo_db, mongo_collection)
    api_streamer = FakeAPIStreamer(producer, kafka_topic)

    # Start API streamer in a separate thread
    api_thread = threading.Thread(target=stream_api_data, args=(api_streamer,))
    api_thread.start()

    try:
        consume_and_write(consumer, writer)
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        api_thread.join()
        producer.close()
        consumer.close()
        writer.close()


if __name__ == "__main__":
    main()
