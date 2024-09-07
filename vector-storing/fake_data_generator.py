import json
import random

from faker import Faker
from loguru import logger
from sentence_transformers import SentenceTransformer

from storing import QdrantHandler

fake = Faker()


class FakeDataGenerator:
    def __init__(self):
        logger.info("Initializing SentenceTransformer model")
        self.encoder = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
        logger.info("SentenceTransformer model loaded successfully")

    def generate_fake_embedding(self, text):
        return self.encoder.encode(text).tolist()

    def generate_fake_data(self, num_records=10):
        fake_data = []
        for _ in range(num_records):
            company_name = fake.company()
            description = fake.paragraph(nb_sentences=3)

            embedding_text = f"{company_name} is a company. {description}"

            startup = {
                "id": fake.uuid4(),
                "embedding": self.generate_fake_embedding(embedding_text),
                "metadata": json.dumps(
                    {
                        "name": company_name,
                        "founded_year": random.randint(2000, 2023),
                        "employees": random.randint(1, 1000),
                        "funding": random.choice(
                            [
                                None,
                                f"${random.randint(100, 1000)}K",
                                f"${random.randint(1, 100)}M",
                            ]
                        ),
                    }
                ),
                "post": description,
            }
            fake_data.append(startup)
        return fake_data


def simulate_kafka_messages(handler, num_messages=10):
    generator = FakeDataGenerator()
    fake_data = generator.generate_fake_data(num_messages)
    for data in fake_data:
        handler.insert_point(data)


if __name__ == "__main__":
    logger.info("Starting fake data insertion process")

    qdrant_handler = QdrantHandler(
        qdrant_url="http://localhost:6333",
        collection_name="startups",
        kafka_bootstrap_servers=["localhost:9093"],
        kafka_topic="spark_to_qdrant",
        quantization_type="scalar",
    )

    qdrant_handler.ensure_collection_exists()
    simulate_kafka_messages(qdrant_handler, num_messages=20)

    logger.info("Fake data insertion process completed")
