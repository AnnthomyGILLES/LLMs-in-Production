import numpy as np
from InstructorEmbedding import INSTRUCTOR
from sentence_transformers import SentenceTransformer


def create_embedding(text: str):
    model = SentenceTransformer(
        "all-MiniLM-L6-v2", device="cpu"
    )
    return model.encode(text)


def embedd_repositories(text: str):
    model = INSTRUCTOR("hkunlp/instructor-xl")
    sentence = text
    instruction = "Represent the structure of the repository"
    return model.encode([instruction, sentence])


def main():
    simple_text = "This is a simple sentence for embedding."
    simple_embedding = create_embedding(simple_text)
    print("Simple embedding shape:", np.array(simple_embedding).shape)
    # print("First few values of simple embedding:", simple_embedding[:5])


if __name__ == "__main__":
    main()
