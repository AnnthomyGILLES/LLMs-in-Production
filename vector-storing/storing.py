import uuid

from loguru import logger
from qdrant_client import models, QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse
from sentence_transformers import SentenceTransformer

from common.kafka_utils.kafka_consumer import KafkaConsumerWrapper


class QdrantHandler:
    _instance = None

    def __init__(
        self,
        qdrant_url,
        collection_name,
        kafka_bootstrap_servers,
        kafka_topic,
        quantization_type=None,
        encoder_model="all-MiniLM-L6-v2",
    ):
        if self._instance is None:
            try:
                self._instance = QdrantClient(qdrant_url)
            except UnexpectedResponse:
                logger.exception("Couldn't connect to the database.")
                raise
        self.collection_name = collection_name
        self.encoder = SentenceTransformer(encoder_model, device="cpu")
        self.consumer = KafkaConsumerWrapper(kafka_bootstrap_servers, kafka_topic)
        self.quantization_type = quantization_type

    def ensure_collection_exists(self):
        if not self._instance.collection_exists(collection_name=self.collection_name):
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

            self._instance.create_collection(
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
        embedding_text = data["description"]
        embedding = self.encoder.encode(embedding_text).tolist()

        point = models.PointStruct(
            id=str(uuid.uuid4()),
            vector={"default": embedding},
            payload={
                "name": data["name"],
                "images": data["images"],
                "alt": data["alt"],
                "description": data["description"],
                "link": data["link"],
                "city": data["city"],
            },
        )
        self._instance.upsert(collection_name=self.collection_name, points=[point])
        logger.info(f"Successfully inserted/upserted point with name: {data['name']}")

    def process_messages(self):
        self.ensure_collection_exists()
        logger.info("Starting to process messages from Kafka")
        for message in self.consumer.consume():
            try:
                data = message["value"]
                logger.debug(f"Received message: {data}")
                self.insert_point(data)
            except Exception as e:
                logger.error(f"Failed to process message with error: {e}")

    def close(self):
        self.consumer.close()
        if self._instance:
            self._instance.close()
        logger.info("Kafka consumer and Qdrant client connections closed.")


if __name__ == "__main__":
    logger.info("Starting process to consume from Kafka and insert into Qdrant")

    qdrant_handler = QdrantHandler(
        qdrant_url="http://localhost:6333",
        collection_name="startups",
        kafka_bootstrap_servers=["localhost:9093"],
        kafka_topic="qdrant_startups",
        quantization_type=None,
    )
    try:
        qdrant_handler.process_messages()
    except KeyboardInterrupt:
        logger.info("Received interrupt, closing consumer...")
    finally:
        qdrant_handler.close()

    logger.info("Process completed")
