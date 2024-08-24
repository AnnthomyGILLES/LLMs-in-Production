import pandas as pd
from qdrant_client import models, QdrantClient
from qdrant_client.models import PointStruct  # Import the PointStruct to store the vector and payload
from sentence_transformers import SentenceTransformer
from tqdm import tqdm  # Library to show the progress bar

model = SentenceTransformer("all-MiniLM-L6-v2")

if __name__ == '__main__':
    # with zipfile.ZipFile("vector_database_wikipedia_articles_embedded.zip", "r") as zip_ref:
    #     zip_ref.extractall("../data")

    article_df = pd.read_csv('../data/wikipedia.csv',
                             usecols=["url", "title", "text"])

    # Create embeddings for titles
    title_embeddings = model.encode(article_df['title'].tolist())

    # Create embeddings for content
    content_embeddings = model.encode(article_df['text'].tolist())

    # Add embeddings to the DataFrame
    article_df['title_vector'] = title_embeddings.tolist()
    article_df['content_vector'] = content_embeddings.tolist()

    article_df.info(show_counts=True)

    # Get the vector size from the first row to set up the collection
    vector_size = len(article_df['content_vector'][0])
    qdrant = QdrantClient("http://localhost:6333")

    # Set up the collection with the vector configuration. You need to declare the vector size and distance metric for the collection. Distance metric enables vector database to index and search vectors efficiently.
    if not qdrant.collection_exists(collection_name="wikipedia"):
        qdrant.create_collection(
            collection_name="wikipedia",
            vectors_config=models.VectorParams(
                size=model.get_sentence_embedding_dimension(),  # Vector size is defined by used model
                distance=models.Distance.COSINE,
            ),
        )

    # Populate collection with vectors using tqdm to show progress
    for k, v in tqdm(article_df.iterrows(), desc="Upserting articles", total=len(article_df)):
        try:
            qdrant.upsert(
                collection_name='wikipedia',
                points=[
                    PointStruct(
                        id=k,
                        vector={'title': v['title_vector'],
                                'content': v['content_vector']},
                        payload={
                            'id': v['id'],
                            'title': v['title'],
                            'url': v['url']
                        }
                    )
                ]
            )
        except Exception as e:
            print(f"Failed to upsert row {k}: {v}")
            print(f"Exception: {e}")
    # Check the collection size to make sure all the points have been stored
    qdrant.count(collection_name='wikipedia')
