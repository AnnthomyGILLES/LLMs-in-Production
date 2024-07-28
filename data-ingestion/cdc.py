import json
import logging

from bson import json_util
from kafka import KafkaProducer

from db.mongo import MongoDatabaseConnector
from kafka_stream.utils import json_serializer

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=json_serializer
)


def stream_process():
    try:
        # Setup MongoDB connection
        client = MongoDatabaseConnector()
        db = client["llmtoprod_db"]
        logging.info("Connected to MongoDB.")

        # Watch changes in a specific collection
        changes = db.watch([{"$match": {"operationType": {"$in": ["insert"]}}}])
        for change in changes:
            data_type = change["ns"]["coll"]
            entry_id = str(change["fullDocument"]["_id"])  # Convert ObjectId to string
            change["fullDocument"].pop("_id")
            change["fullDocument"]["type"] = data_type
            change["fullDocument"]["entry_id"] = entry_id

            # Use json_util to serialize the document
            data = json.dumps(change["fullDocument"], default=json_util.default)
            logging.info(f"Change detected and serialized: {data}")

            # Send data to rabbitmq
            logging.info("Data published to Kafka.")
            producer.send('outgoing-data', value=data)


    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    stream_process()
