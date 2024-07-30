import json
import logging

from bson import json_util
from pymongo.errors import PyMongoError

from mongodb_writer import MongoDBWriter


class ChangeDataCapture:
    def __init__(self, mongo_client, kafka_producer):
        self.mongo_client = mongo_client
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_producer.topic
        logging.info(f"CDC will publish changes to Kafka topic: {self.kafka_topic}")

    def start_watching(self):
        logging.info("Starting to watch for changes...")
        try:
            with self.mongo_client.collection.watch() as stream:
                for change in stream:
                    self.process_change(change)
        except PyMongoError as e:
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
            self.send_to_kafka(data)
            logging.info(f"Processed {operation} operation for document: {entry_id}")

    def send_to_kafka(self, change_data):
        success = self.kafka_producer.send_message(change_data)
        if success:
            logging.info(f"Sent change data to Kafka topic: {self.kafka_topic}")
        else:
            logging.error("Failed to send change data to Kafka")

    def close(self):
        self.mongo_client.close()
        self.kafka_producer.close()
        logging.info("CDC connections closed.")


if __name__ == "__main__":
    from kafka_producer import KafkaProducerWrapper

    bootstrap_servers = ['localhost:9093']
    kafka_topic = 'outgoing-data'
    mongo_uri = "mongodb://localhost:27017"
    mongo_db = "llmtoprod_db"
    mongo_collection = "kafka_messages"
    mongo_client = MongoDBWriter(mongo_uri, mongo_db, mongo_collection)
    kafka_producer = KafkaProducerWrapper(bootstrap_servers, kafka_topic)
    cdc = ChangeDataCapture(mongo_client, kafka_producer)
    try:
        cdc.start_watching()
    except KeyboardInterrupt:
        logging.info("CDC stopped by user.")
    finally:
        cdc.close()
