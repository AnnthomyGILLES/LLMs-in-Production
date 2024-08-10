import json

from loguru import logger
from qdrant_client import models, QdrantClient
from sentence_transformers import SentenceTransformer

from fake_kafka_consumer import KafkaConsumerWrapper


def process_and_insert_to_qdrant():
    bootstrap_servers = ['redpanda:29092']
    kafka_topic = 'output-spark-topic'

    logger.info(f"Setting up Kafka Consumer for topic '{kafka_topic}' with bootstrap servers {bootstrap_servers}")
    consumer = KafkaConsumerWrapper(bootstrap_servers, kafka_topic)
    logger.info("Kafka Consumer successfully instantiated")

    logger.info("Setting up SentenceTransformer model")
    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    logger.info("SentenceTransformer model loaded successfully")

    logger.info("Initializing Qdrant client")
    client = QdrantClient("http://qdrant:6333")
    logger.info("Qdrant client initialized successfully")

    collection_name = "startups"
    if not client.collection_exists(collection_name=collection_name):
        logger.info(f"Creating collection '{collection_name}' in Qdrant")
        client.create_collection(
            collection_name=collection_name,
            vectors_config={"default": models.VectorParams(size=encoder.get_sentence_embedding_dimension(),
                                                           distance=models.Distance.COSINE)},
        )
        logger.info(f"Collection '{collection_name}' created successfully")
    else:
        logger.info(f"Collection '{collection_name}' already exists")

    logger.info("Starting to process messages from Kafka")
    for message in consumer.consumer:
        try:
            data = message.value
            logger.debug(f"Received message: {data}")

            point = models.PointStruct(
                id=data['id'],
                vector={"default": data['embedding']},
                payload={
                    "metadata": json.loads(data['metadata']),
                    "post": data['post']
                }
            )

            client.upsert(
                collection_name=collection_name,
                points=[point]
            )

            logger.info(f"Successfully inserted point with ID: {data['id']}")

        except Exception as e:
            logger.error(f"Failed to process message with error: {e}")


if __name__ == "__main__":
    logger.add("qdrant_insert.log", rotation="10 MB")
    logger.info("Starting process to consume from Kafka and insert into Qdrant")
    process_and_insert_to_qdrant()
    logger.info("Process completed")
