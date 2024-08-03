# Import client library
from qdrant_client import models, QdrantClient
from qdrant_client.grpc import VectorParams, Distance
from sentence_transformers import SentenceTransformer


def write_to_qdrant(batch_df):
    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    client = QdrantClient("http://qdrant:6333")

    if not client.collection_exists(collection_name="{startups}"):
        client.create_collection(
            collection_name="startups",
            vectors_config=VectorParams(size=encoder.get_sentence_embedding_dimension(), distance=Distance.COSINE),
        )
    points = []
    for row in batch_df.collect():
        point = models.PointStruct(
            id=row['id'],
            vector=row['embedding'],
            payload={
                "metadata": row['metadata'],
                "post": row['post']
            }
        )
        
        points.append(point)

    # Upsert points in batches
    client.upsert(
        collection_name="startups",
        points=points
    )
