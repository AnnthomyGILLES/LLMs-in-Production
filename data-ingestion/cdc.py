import json
import logging

import pymongo
from bson import json_util
from kafka import KafkaProducer

from mongodb_writer import MongoDBWriter


class ChangeDataCapture:
    def __init__(self, mongo_client, kafka_servers, kafka_topic):
        self.mongo_client = mongo_client
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.kafka_topic = kafka_topic
        logging.info(f"CDC initialized for MongoDB collection: {database}.{collection}")
        logging.info(f"CDC will publish changes to Kafka topic: {kafka_topic}")

    def start_watching(self):
        logging.info("Starting to watch for changes...")
        try:
            with self.mongo_client.collection.watch() as stream:
                for change in stream:
                    self.process_change(change)
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error watching MongoDB changes: {str(e)}")

    def process_change(self, change):
        operation = change['operationType']
        if operation in ['insert', 'update', 'replace', 'delete']:
            data_type = change["ns"]["coll"]
            entry_id = str(change["fullDocument"]["_id"])
            change["fullDocument"].pop("_id")
            change["fullDocument"]["type"] = data_type
            change["fullDocument"]["entry_id"] = entry_id
            full_document = change.get('fullDocument')
            data = json.dumps(full_document, default=json_util.default)
            logging.info(f"Change detected and serialized: {data}")

            logging.info("Data published to Kafka.")
            self.send_to_kafka(data)

            logging.info(f"Processed {operation} operation for document: {entry_id}")

    # TODO Replace with a dedicated KafkaProducer class
    def send_to_kafka(self, change_data):
        try:
            self.kafka_producer.send(self.kafka_topic, change_data)
            self.kafka_producer.flush()
            logging.info(f"Sent change data to Kafka topic: {self.kafka_topic}")
        except Exception as e:
            logging.error(f"Error sending to Kafka: {str(e)}")

    def close(self):
        self.mongo_client.close()
        self.kafka_producer.close()
        logging.info("CDC connections closed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    mongo_uri = "mongodb://localhost:27017"
    database = "llmtoprod_db"
    collection = "kafka_messages"
    kafka_servers = ['localhost:9093']
    kafka_topic = 'outgoing-data'

    writer = MongoDBWriter(mongo_uri, database, collection)
    cdc = ChangeDataCapture(writer, kafka_servers, kafka_topic)

    try:
        cdc.start_watching()
    except KeyboardInterrupt:
        logging.info("CDC stopped by user.")
    finally:
        cdc.close()
