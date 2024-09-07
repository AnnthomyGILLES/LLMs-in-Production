from loguru import logger
from pymongo.errors import PyMongoError

from mongodb_writer import MongoDBWriter
from common.kafka_utils.kafka_producer import KafkaProducerWrapper


class DebeziumMongoDBCDC:
    def __init__(self, mongo_writer, kafka_producer):
        self.mongo_writer = mongo_writer
        self.kafka_producer = kafka_producer
        logger.info(
            f"MongoDB CDC initialized. Database: {mongo_writer.db.name}, Collection: {mongo_writer.collection.name}"
        )
        logger.info(f"Kafka producer initialized. Topic: {kafka_producer.topic}")

    def start_watching(self):
        logger.info("Starting to watch for changes...")
        try:
            with self.mongo_writer.collection.watch() as stream:
                for change in stream:
                    self.process_change(change)
        except PyMongoError as e:
            logger.error(f"Error watching MongoDB changes: {str(e)}")

    def process_change(self, change):
        operation = change["operationType"]
        if operation in ["insert", "update", "replace", "delete"]:
            document = change.get("fullDocument", {})
            document_id = str(document.get("_id", ""))

            debezium_message = {
                "before": None,
                "after": document if operation != "delete" else None,
                "source": {
                    "version": "1.0",
                    "connector": "mongodb",
                    "name": "mongodb1",
                    "ts_ms": change["clusterTime"].time * 1000,
                    "snapshot": "false",
                    "db": change["ns"]["db"],
                    "collection": change["ns"]["coll"],
                    "ord": change["documentKey"]["_id"],
                },
                "op": operation[0].upper(),
                "ts_ms": int(change["clusterTime"].time * 1000),
            }

            self.send_to_kafka(debezium_message)
            logger.info(f"Processed {operation} operation for document: {document_id}")

    def send_to_kafka(self, message):
        success = self.kafka_producer.send_message(message)
        if success:
            logger.info(f"Sent change data to Kafka topic: {self.kafka_producer.topic}")
        else:
            logger.error("Failed to send change data to Kafka")

    def close(self):
        self.mongo_writer.close()
        self.kafka_producer.close()
        logger.info("CDC connections closed.")


if __name__ == "__main__":
    mongo_uri = "mongodb://localhost:27017/?replicaSet=rs0"
    mongo_db = "llmtoprod_db"
    mongo_collection = "articles"
    kafka_bootstrap_servers = ["localhost:9093"]
    kafka_topic = "mongodb_cdc"

    mongo_writer = MongoDBWriter(mongo_uri, mongo_db, mongo_collection)
    kafka_producer = KafkaProducerWrapper(kafka_bootstrap_servers, kafka_topic)

    cdc = DebeziumMongoDBCDC(mongo_writer, kafka_producer)

    try:
        cdc.start_watching()
    except KeyboardInterrupt:
        logger.info("CDC stopped by user.")
    finally:
        cdc.close()
