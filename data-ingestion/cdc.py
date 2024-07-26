import threading

from bson.json_util import dumps
from pymongo.errors import PyMongoError

from db.mongo import connection


def watch_collection(collection_name, db):
    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]

    try:
        collection = db[collection_name]
        with collection.watch(pipeline) as stream:
            print(f"Watching collection: {collection_name}")
            for change in stream:
                print(f"Change in {collection_name}:")
                print(dumps(change, indent=2))
                # Here you can add logic to process the change
                # For example, you could send it to another system or update a cache
    except PyMongoError as e:
        print(f"Error watching {collection_name}: {e}")


def watch_database():
    try:
        db = connection.get_database("scrabble")

        # Get all collection names
        collection_names = db.list_collection_names()

        # Create a thread for each collection
        threads = []
        for collection_name in collection_names:
            thread = threading.Thread(target=watch_collection, args=(collection_name, db))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()
    except Exception as e:
        print(f"Error in watch_database: {e}")
    finally:
        connection.close()


if __name__ == "__main__":
    watch_database()
