import pandas as pd
from loguru import logger
from qdrant_client import models, QdrantClient
from qdrant_client.models import PointStruct  # Import the PointStruct to store the vector and payload
from sentence_transformers import SentenceTransformer
from tqdm import tqdm  # Library to show the progress bar

encoder = SentenceTransformer("all-MiniLM-L6-v2")
client = QdrantClient("http://localhost:6333")


def upsert_wikipedia_to_db():
    try:
        logger.info("Starting the embedding and storage process")

        article_df = pd.read_csv('../data/wikipedia.csv', usecols=["url", "title", "text"])
        article_df = article_df.head(1000)
        logger.info(f"Loaded {len(article_df)} articles from CSV")

        logger.info("Creating embeddings for content")
        content_embeddings = encoder.encode(article_df['text'].tolist())

        logger.info("Adding embeddings to the DataFrame")
        article_df['content_vector'] = content_embeddings.tolist()

        logger.info("DataFrame info:")
        article_df.info(show_counts=True)

        logger.info("Connected to Qdrant client")

        if not client.collection_exists(collection_name="wikipedia"):
            logger.info("Creating 'wikipedia' collection in Qdrant")
            client.create_collection(
                collection_name="wikipedia",
                vectors_config={
                    'content': models.VectorParams(
                        size=encoder.get_sentence_embedding_dimension(),
                        distance=models.Distance.COSINE,
                    ),
                },
            )
        else:
            logger.info("'wikipedia' collection already exists in Qdrant")

        logger.info("Starting to upsert articles to Qdrant")
        for k, v in tqdm(article_df.iterrows(), desc="Upserting articles", total=len(article_df)):
            try:
                client.upsert(
                    collection_name='wikipedia',
                    points=[
                        PointStruct(
                            id=k,
                            vector={'content': v['content_vector']},
                            payload={
                                'title': v['title'],
                                'url': v['url'],
                                'article': v['text'],
                            }
                        )
                    ]
                )
            except Exception as e:
                logger.error(f"Failed to upsert row {k}: {v}")
                logger.exception(f"Exception: {e}")

        collection_size = client.count(collection_name='wikipedia')
        logger.info(f"Final collection size: {collection_size}")

        logger.info("Process completed successfully")

    except Exception as e:
        logger.critical("An unexpected error occurred")
        logger.exception(f"Exception: {e}")


if __name__ == '__main__':
    upsert_wikipedia_to_db()
    # hits = client.search(
    #     collection_name="wikipedia",
    #     query_vector=("content", encoder.encode("Europe").tolist()),
    #     limit=3,
    # )
    # for hit in hits:
    #     print(hit.payload, "score:", hit.score)
