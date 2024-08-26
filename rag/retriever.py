from typing import List

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

    def retrieve_with_context_overlap(self, query: str, num_neighbors: int = 1, chunk_size: int = 200,
                                      chunk_overlap: int = 20) -> List[str]:
        """
        Retrieve chunks based on a query, then fetch neighboring chunks and concatenate them, 
        accounting for overlap and correct indexing.

        :param query: The query to search for relevant chunks.
        :param num_neighbors: The number of chunks to retrieve before and after each relevant chunk.
        :param chunk_size: The size of each chunk when originally split.
        :param chunk_overlap: The overlap between chunks when originally split.
        :return: List of concatenated chunk sequences, each centered on a relevant chunk.
        """
        query_vector = self.get_embedding(query)

        # Search for the most relevant chunks
        search_result = self.qdrant_client.search(
            collection_name=self.collection_name,
            query_vector=("content", query_vector),
            limit=5  # Adjust this number based on how many relevant chunks you want to consider
        )

        result_sequences = []

        for hit in search_result:
            current_index = hit.id
            if current_index is None:
                continue

            # Determine the range of chunks to retrieve
            start_index = max(0, current_index - num_neighbors)
            end_index = current_index + num_neighbors + 1  # +1 because range is exclusive at the end

            # Retrieve all chunks in the range
            neighbor_chunks = []
            for i in range(start_index, end_index):
                neighbor_chunk = self.get_chunk_by_index(i)
                if neighbor_chunk:
                    neighbor_chunks.append(neighbor_chunk)

            # Sort chunks by their index to ensure correct order
            neighbor_chunks.sort(key=lambda x: x.id)

            # Concatenate chunks, accounting for overlap
            concatenated_text = neighbor_chunks[0].payload["article"]
            for i in range(1, len(neighbor_chunks)):
                current_chunk = neighbor_chunks[i].payload["article"]
                overlap_start = max(0, len(concatenated_text) - chunk_overlap)
                concatenated_text = concatenated_text[:overlap_start] + current_chunk

            result_sequences.append(concatenated_text)

        return result_sequences

    def get_chunk_by_index(self, index: int):
        """
        Retrieve a chunk from Qdrant by its index.

        :param index: The index of the chunk to retrieve.
        :return: The chunk data including its payload, or None if not found.
        """
        # Search for the chunk with the specific index
        search_result = self.qdrant_client.retrieve(
            collection_name=self.collection_name,
            ids=[index]
        )

        if search_result:
            return search_result[0]
        return None


if __name__ == '__main__':
    retriever = RAGRetriever(
        qdrant_host="localhost",
        qdrant_port=6333,
        collection_name="wikipedia"
    )

    query = "Europe"

    # Standard retrieval
    results = retriever.retrieve(query, top_k=3)
    print("Standard Retrieval Results:")
    for doc in results:
        print(f"Content: {doc['content'][:100]}...")  # Truncated for brevity
        print(f"Metadata: {doc['metadata']}")
        print(f"Relevance Score: {doc['score']}")
        print("---")

    # Retrieval with context overlap
    context_results = retriever.retrieve_with_context_overlap(query, num_neighbors=1)
    print("\nRetrieval with Context Overlap Results:")
    for i, result in enumerate(context_results):
        print(f"Result {i + 1}: {result[:200]}...")  # Truncated for brevity
        print("---")
