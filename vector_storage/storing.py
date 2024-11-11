import uuid
from typing import List, Dict, Any, Optional

from loguru import logger
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    SparseIndexParams,
    SparseVectorParams,
    ScalarQuantization,
    ScalarQuantizationConfig,
    BinaryQuantization,
    BinaryQuantizationConfig,
    ProductQuantization,
    ProductQuantizationConfig,
)
from sentence_transformers import SentenceTransformer

from common.kafka_utils.kafka_consumer import KafkaConsumerWrapper


class QdrantHandler:
    def __init__(
        self,
        qdrant_url: str,
        collection_name: str,
        kafka_bootstrap_servers: List[str],
        kafka_topic: str,
        encoder_model: str = "all-MiniLM-L6-v2",
        use_hybrid: bool = False,
        sparse_encoder_model: Optional[str] = None,
        quantization_type: Optional[str] = None,
    ):
        self.client = QdrantClient(qdrant_url)
        self.collection_name = collection_name
        self.encoder = SentenceTransformer(encoder_model, device="cpu")
        self.consumer = KafkaConsumerWrapper(kafka_bootstrap_servers, kafka_topic)
        self.use_hybrid = use_hybrid
        self.quantization_type = quantization_type

        if use_hybrid and not sparse_encoder_model:
            raise ValueError(
                "sparse_encoder_model must be provided when use_hybrid is True"
            )

        self.sparse_encoder = (
            SentenceTransformer(sparse_encoder_model, device="cpu")
            if use_hybrid
            else None
        )

    def ensure_collection_exists(self):
        if not self.client.collection_exists(collection_name=self.collection_name):
            logger.info(f"Creating collection '{self.collection_name}' in Qdrant")

            quantization_config = self._get_quantization_config()

            vectors_config = {
                "default": VectorParams(
                    size=self.encoder.get_sentence_embedding_dimension(),
                    distance=Distance.COSINE,
                    quantization_config=quantization_config,
                )
            }

            sparse_vectors_config = None
            if self.use_hybrid:
                sparse_vectors_config = {
                    "sparse": SparseVectorParams(index=SparseIndexParams(on_disk=False))
                }

            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=vectors_config,
                sparse_vectors_config=sparse_vectors_config,
            )
            logger.info(f"Collection '{self.collection_name}' created successfully")
        else:
            logger.info(f"Collection '{self.collection_name}' already exists")

    def _get_quantization_config(self):
        if self.quantization_type == "scalar":
            return ScalarQuantization(scalar=ScalarQuantizationConfig(type="int8"))
        elif self.quantization_type == "binary":
            return BinaryQuantization(binary=BinaryQuantizationConfig(encode_length=8))
        elif self.quantization_type == "product":
            return ProductQuantization(
                product=ProductQuantizationConfig(num_subvectors=16)
            )
        return None

    def insert_point(self, data: Dict[str, Any]):
        embedding_text = data["description"]
        dense_vector = self.encoder.encode(embedding_text).tolist()

        point = {
            "id": str(uuid.uuid4()),
            "vector": {"default": dense_vector},
            "payload": {
                "name": data["name"],
                "images": data["images"],
                "alt": data["alt"],
                "description": data["description"],
                "link": data["link"],
                "city": data["city"],
            },
        }

        if self.use_hybrid:
            sparse_vector = self.sparse_encoder.encode(
                embedding_text, output_value="sparse_tensor"
            )
            point["vector"]["sparse"] = {
                "indices": sparse_vector.indices().tolist(),
                "values": sparse_vector.values().tolist(),
            }

        self.client.upsert(
            collection_name=self.collection_name, points=[PointStruct(**point)]
        )
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

    def search(self, query: str, limit: int = 10):
        dense_vector = self.encoder.encode(query).tolist()
        search_params = {
            "collection_name": self.collection_name,
            "query_vector": ("default", dense_vector),
            "limit": limit,
        }

        if self.use_hybrid:
            sparse_vector = self.sparse_encoder.encode(
                query, output_value="sparse_tensor"
            )
            search_params["query_vector"] = [
                ("default", dense_vector),
                (
                    "sparse",
                    {
                        "indices": sparse_vector.indices().tolist(),
                        "values": sparse_vector.values().tolist(),
                    },
                ),
            ]

        return self.client.search(**search_params)

    def close(self):
        self.consumer.close()
        self.client.close()
        logger.info("Kafka consumer and Qdrant client connections closed.")


if __name__ == "__main__":
    logger.info("Starting process to consume from Kafka and insert into Qdrant")

    qdrant_handler = QdrantHandler(
        qdrant_url="http://localhost:6333",
        collection_name="startups",
        kafka_bootstrap_servers=["localhost:9093"],
        kafka_topic="qdrant_startups",
        use_hybrid=False,  # Set to True to use hybrid search
        sparse_encoder_model=None,
        # Only needed if use_hybrid is True
        quantization_type=None,  # Can be "scalar", "binary", "product", or None
    )
    try:
        qdrant_handler.process_messages()
    except KeyboardInterrupt:
        logger.info("Received interrupt, closing consumer...")
    finally:
        qdrant_handler.close()

    logger.info("Process completed")
