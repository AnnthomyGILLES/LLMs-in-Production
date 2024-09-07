import json

from loguru import logger
from qdrant_client import models, QdrantClient
from sentence_transformers import SentenceTransformer

from common.kafka_utils.kafka_consumer import KafkaConsumerWrapper


class QdrantHandler:
    def __init__(
        self,
        qdrant_url,
        collection_name,
        kafka_bootstrap_servers,
        kafka_topic,
        quantization_type=None,
        encoder_model="all-MiniLM-L6-v2",
    ):
        self.client = QdrantClient(qdrant_url)
        self.collection_name = collection_name
        self.encoder = SentenceTransformer(encoder_model, device="cpu")
        self.consumer = KafkaConsumerWrapper(kafka_bootstrap_servers, kafka_topic)
        self.quantization_type = quantization_type

    def ensure_collection_exists(self):
        if not self.client.collection_exists(collection_name=self.collection_name):
            logger.info(f"Creating collection '{self.collection_name}' in Qdrant")

            quantization_config = None
            if self.quantization_type == "scalar":
                quantization_config = models.ScalarQuantization(
                    scalar=models.ScalarQuantizationConfig(type="int8")
                )
            elif self.quantization_type == "binary":
                quantization_config = models.BinaryQuantization(
                    binary=models.BinaryQuantizationConfig(encode_length=8)
                )
            elif self.quantization_type == "product":
                quantization_config = models.ProductQuantization(
                    product=models.ProductQuantizationConfig(num_subvectors=16)
                )

            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config={
                    "default": models.VectorParams(
                        size=self.encoder.get_sentence_embedding_dimension(),
                        distance=models.Distance.COSINE,
                        quantization_config=quantization_config,
                    )
                },
            )
            logger.info(f"Collection '{self.collection_name}' created successfully")
        else:
            logger.info(f"Collection '{self.collection_name}' already exists")

    def insert_point(self, data):
        point = models.PointStruct(
            id=data["id"],
            vector={"default": data["embedding"]},
            payload={"metadata": json.loads(data["metadata"]), "post": data["post"]},
        )
        self.client.upsert(collection_name=self.collection_name, points=[point])
        logger.info(f"Successfully inserted point with ID: {data['id']}")

    def process_messages(self):
        self.ensure_collection_exists()
        logger.info("Starting to process messages from Kafka")
        for message in self.consumer.consumer:
            try:
                data = message.value
                logger.debug(f"Received message: {data}")
                self.insert_point(data)
            except Exception as e:
                logger.error(f"Failed to process message with error: {e}")


def main():
    logger.info("Starting process to consume from Kafka and insert into Qdrant")

    qdrant_handler = QdrantHandler(
        qdrant_url="http://qdrant:6333",
        collection_name="startups",
        kafka_bootstrap_servers=["redpanda:29092"],
        kafka_topic="output-spark-topic",
        quantization_type="scalar",
    )
    qdrant_handler.process_messages()

    logger.info("Process completed")


if __name__ == "__main__":
    main()
