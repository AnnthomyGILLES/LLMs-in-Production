from loguru import logger
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class MongoDBWriter:
    _instance = None
    _client = None

    def __new__(cls, uri, database, collection):
        if cls._instance is None:
            cls._instance = super(MongoDBWriter, cls).__new__(cls)
            try:
                cls._client = MongoClient(uri)
                cls._instance.db = cls._client[database]
                cls._instance.collection = cls._instance.db[collection]
                logger.info(
                    f"MongoDB connector initialized. Database: {database}, Collection: {collection}"
                )
            except ConnectionFailure as e:
                logger.error(f"Couldn't connect to the database: {str(e)}")
                raise
        return cls._instance

    def write(self, data):
        try:
            result = self.collection.insert_one(data)
            logger.info(f"Data written to MongoDB. Inserted ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Error writing to MongoDB: {str(e)}")
            raise

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            raise RuntimeError(
                "MongoDB Writer not initialized. Create an instance first."
            )
        return cls._instance

    @classmethod
    def close(cls):
        if cls._client:
            cls._client.close()
            cls._client = None
            cls._instance = None
            logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    writer1 = MongoDBWriter(
        "mongodb://localhost:27017", "llmtoprod_db", "kafka_messages"
    )

    writer2 = MongoDBWriter(
        "mongodb://localhost:27017", "llmtoprod_db", "kafka_messages"
    )

    assert writer1 is writer2

    # Write some data
    writer1.write({"test": "data1"})
    writer2.write({"test": "data2"})

    # Close the connection (can be called from either instance)
    writer1.close()
