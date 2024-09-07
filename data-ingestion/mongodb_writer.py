from loguru import logger
from pymongo import MongoClient


class MongoDBWriter:
    def __init__(self, uri, database, collection):
        self.client = MongoClient(uri)
        self.db = self.client[database]
        self.collection = self.db[collection]
        logger.info(
            f"MongoDB connector initialized. Database: {database}, Collection: {collection}"
        )

    def write(self, data):
        try:
            result = self.collection.insert_one(data)
            logger.info(f"Data written to MongoDB. Inserted ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Error writing to MongoDB: {str(e)}")
            raise

    def close(self):
        self.client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    connector = MongoDBWriter(
        "mongodb://localhost:27017", "llmtoprod_db", "kafka_messages"
    )
    connector.write({"test": "data"})
    connector.close()
