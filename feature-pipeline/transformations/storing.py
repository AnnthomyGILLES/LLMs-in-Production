import json

from qdrant_client import models, QdrantClient
from qdrant_client.grpc import VectorParams
from sentence_transformers import SentenceTransformer

from fake_kafka_consumer import KafkaConsumerWrapper


def process_and_insert_to_qdrant():
    # Kafka Consumer setup
    bootstrap_servers = ['redpanda:29092']
    kafka_topic = 'output-spark-topic'
    consumer = KafkaConsumerWrapper(bootstrap_servers, kafka_topic)

    # Qdrant setup
    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    client = QdrantClient("http://qdrant:6333")

    # Create collection if it doesn't exist
    if not client.collection_exists(collection_name="startups"):
        client.create_collection(
            collection_name="startups",
            vectors_config=VectorParams(size=encoder.get_sentence_embedding_dimension(),
                                        distance=models.Distance.COSINE),
        )

    # Process messages and insert into Qdrant
    for message in consumer:
        data = message.value

        point = models.PointStruct(
            id=data['id'],
            vector=data['embedding'],
            payload={
                "metadata": json.loads(data['metadata']),
                "post": data['post']
            }
        )

        # Upsert point
        client.upsert(
            collection_name="startups",
            points=[point]
        )

        print(f"Inserted point with ID: {data['id']}")


if __name__ == "__main__":
    process_and_insert_to_qdrant()
