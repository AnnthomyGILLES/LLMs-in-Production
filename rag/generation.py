import os

import numpy as np
from chromadb.utils import embedding_functions
from dotenv import load_dotenv
from openai import OpenAI
from sentence_transformers import CrossEncoder

from config import config
from retrieval import augment_query_generated
from storage import StoreResults
from tools import word_wrap, call_openai

load_dotenv()
openai_client = OpenAI(api_key=os.getenv("OPENAI_API"))

cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")


class ResearchAssistant:
    """
    A class that acts as a research assistant using various NLP models and techniques.

    This class encapsulates functionality for augmenting queries, retrieving relevant documents,
    and generating responses based on those documents. It utilizes a sentence transformer for embeddings,
    a retrieval model for fetching documents, and an OpenAI model for generating responses.

    Attributes:
        embedding_function (SentenceTransformerEmbeddingFunction): Embedding function for processing queries.
        model (str): Name of the retrieval model used for document fetching.
        store (StoreResults): Instance for storing and retrieving results.
    """

    def __init__(self):
        """
        Initialize the ResearchAssistant class with required models and storage mechanisms.
        """
        self.embedding_function = (
            embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name=config["models"]["embedding"]
            )
        )
        self.model = config["models"]["retrieval"]
        self.store = StoreResults()

    def augment_query(self, query):
        """
        Augment a given query using a language model.

        Args:
            query (str): The original query to be augmented.

        Returns:
            str: The augmented query.
        """
        return augment_query_generated(query)

    def retrieve_documents(self, query, n_results=5):
        """
        Retrieve a set of documents relevant to the given query.

        Args:
            query (str): The query for which relevant documents are to be retrieved.

        Returns:
            list: A list of retrieved documents.
        """
        results = self.store.collection.query(
            query_texts=[query],
            n_results=n_results,
            include=["documents", "embeddings"],
        )
        return results["documents"][0]

    def generate_response(self, query, retrieved_documents):
        """
        Generate a response to a query based on a set of retrieved documents.

        Args:
            query (str): The original query.
            retrieved_documents (list): A list of documents retrieved based on the query.

        Returns:
            str: The generated response to the query.
        """
        information = "\n\n".join(retrieved_documents)
        messages = [
            {
                "role": "system",
                "content": "You are a helpful expert research assistant in artificial intelligence. Your users are asking questions about information contained in a documentation."
                           "You will be shown the user's question, and the relevant information from documentation. Answer the user's question using only this information.",
            },
            {
                "role": "user",
                "content": f"Question: {query}. \n Information: {information}",
            },
        ]
        response = call_openai(messages, self.model, temperature=0.7)
        return response

    def rerank_documents(self, query, retrieved_documents):
        pairs = [[query, doc] for doc in retrieved_documents]
        similarity_scores = cross_encoder.predict(pairs)
        sim_scores_argsort = np.argsort(similarity_scores)[::-1]
        original_array = np.array(retrieved_documents)
        reordered_docs = original_array[sim_scores_argsort]
        return reordered_docs

    def process_query(self, original_query):
        """
        Process an original query through augmentation, document retrieval, and response generation.

        This method combines the functionalities of augmenting the query, retrieving relevant documents,
        and generating a final response.

        Args:
            original_query (str): The original query to be processed.

        Returns:
            str: The final processed output for the query.
        """
        joint_query = self.augment_query(original_query)
        retrieved_documents = self.retrieve_documents(joint_query)
        reordered_documents = self.rerank_documents(joint_query, retrieved_documents)
        output = self.generate_response(original_query, reordered_documents)
        return word_wrap(output)


if __name__ == "__main__":
    assistant = ResearchAssistant()
    query = "what are the strategies to mitigate hallucination in llm"
    result = assistant.process_query(query)
    print(result)
