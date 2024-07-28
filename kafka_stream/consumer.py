import json

from kafka import KafkaConsumer

from db.mongo import MongoDatabaseConnector


def json_deserializer(data):
    return json.loads(data.decode('utf-8'))


consumer = KafkaConsumer(
    'incoming-data',
    bootstrap_servers=['localhost:9093'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=json_deserializer
)

client = MongoDatabaseConnector()
db = client["llmtoprod_db"]
collection = db['kafka_messages']

if __name__ == '__main__':
    for message in consumer:
        print(f"Received message: {message.value}")
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")

        # Prepare document to insert into MongoDB
        document = {
            'value': message.value,
            'topic': message.topic,
            'partition': message.partition,
            'offset': message.offset,
            'timestamp': message.timestamp
        }

        # Insert document into MongoDB
        result = collection.insert_one(document)

        print(f"Inserted document ID: {result.inserted_id}")
        print("--------------------")
