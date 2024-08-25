import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter
from sentence_transformers import CrossEncoder
from sentence_transformers import SentenceTransformer

cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")


class RAGRetriever:
    def __init__(self, qdrant_host: str, qdrant_port: int, collection_name: str,
                 embedding_model: str = "all-MiniLM-L6-v2"):
        """
        Initialize the RAGRetriever with Qdrant connection and embedding model.

        :param qdrant_host: Hostname of the Qdrant server
        :param qdrant_port: Port number of the Qdrant server
        :param collection_name: Name of the Qdrant collection to query
        :param embedding_model: Name of the sentence-transformers model to use for embeddings
        """
        self.qdrant_client = QdrantClient(host=qdrant_host, port=qdrant_port)
        self.collection_name = collection_name
        self.embedding_model = SentenceTransformer(embedding_model)

    def get_embedding(self, text: str) -> np.ndarray:
        """
        Generate an embedding for the given text.

        :param text: Input text to embed
        :return: Numpy array representing the embedding
        """
        return self.embedding_model.encode(text)

    def retrieve(self, query: str, top_k: int = 5, filter_condition: dict = None) -> list:
        """
        Retrieve the most relevant documents for the given query.

        :param query: User's input query
        :param top_k: Number of top results to retrieve
        :param filter_condition: Optional filter to apply to the search
        :return: List of retrieved documents with their metadata
        """
        query_vector = self.get_embedding(query)

        search_params = {
            "collection_name": self.collection_name,
            "query_vector": ("content", query_vector),
            "limit": top_k
        }

        if filter_condition:
            search_params["query_filter"] = Filter(**filter_condition)

        results = self.qdrant_client.search(**search_params)

        retrieved_docs = []
        for result in results:
            retrieved_docs.append({
                "content": result.payload["article"],
                "metadata": {k: v for k, v in result.payload.items() if k != "article"},
                "score": result.score
            })

        return retrieved_docs

    def rerank_documents(self, query, retrieved_documents):
        pairs = [[query, doc["content"]] for doc in retrieved_documents]
        similarity_scores = cross_encoder.predict(pairs)
        sim_scores_argsort = np.argsort(similarity_scores)[::-1]
        original_array = np.array(retrieved_documents)
        reordered_docs = original_array[sim_scores_argsort]
        return reordered_docs


if __name__ == '__main__':
    retriever = RAGRetriever(
        qdrant_host="localhost",
        qdrant_port=6333,
        collection_name="wikipedia"
    )

    query = "Europe"
    results = retriever.retrieve(query, top_k=3)

    for doc in results:
        print(f"Content: {doc['content']}")
        print(f"Metadata: {doc['metadata']}")
        print(f"Relevance Score: {doc['score']}")
        print("---")
