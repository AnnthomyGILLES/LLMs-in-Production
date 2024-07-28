import logging

from pymongo import MongoClient


class MongoDBWriter:
    def __init__(self, uri, database, collection):
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db[collection]
        logging.info(f"MongoDB writer initialized. Database: {database}, Collection: {collection}")

    def write(self, data):
        try:
            result = self.collection.insert_one(data)
            logging.info(f"Data written to MongoDB. Inserted ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logging.error(f"Error writing to MongoDB: {str(e)}")
            raise

    def close(self):
        self.client.close()
        logging.info("MongoDB connection closed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    writer = MongoDBWriter('mongodb://localhost:27017', 'llmtoprod_db', 'kafka_messages')
    writer.write({"test": "data"})
    writer.close()
