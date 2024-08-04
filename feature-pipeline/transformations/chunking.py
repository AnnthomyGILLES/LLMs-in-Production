from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_text_splitters import SentenceTransformersTokenTextSplitter


def chunk_text(text):
    character_splitter = RecursiveCharacterTextSplitter(
        separators=[
            "\n\n",
            "\n",
            " ",
            ".",
            ",",
            "\u200b",  # Zero-width space
            "\uff0c",  # Fullwidth comma
            "\u3001",  # Ideographic comma
            "\uff0e",  # Fullwidth full stop
            "\u3002",  # Ideographic full stop
            "",
        ],
        chunk_size=500,
        chunk_overlap=0,
        length_function=len,
    )
    text_split = character_splitter.split_text(text)

    token_splitter = SentenceTransformersTokenTextSplitter(
        chunk_overlap=50,
        tokens_per_chunk=256,
        model_name="sentence-transformers/all-MiniLM-L6-v2",
    )
    chunks = []

    for section in text_split:
        chunks.extend(token_splitter.split_text(section))

    return chunks


if __name__ == "__main__":
    # Sample data for sections_ds
    sample_sections = [
        {
            "text": "Section 1 text goes here. More details about section 1.",
            "source": "Source 1",
        },
        {
            "text": "Section 2 has different content. It might be longer or shorter.",
            "source": "Source 2",
        },
        {
            "text": "Another section, Section 3, with its own unique text and information.",
            "source": "Source 3",
        },
    ]

    # Process each section and create chunks
    all_chunks = []
    for section in sample_sections:
        chunks = chunk_text(section, chunk_size=10, chunk_overlap=5)
        all_chunks.extend(chunks)

    # Print the resulting chunks
    for i, chunk in enumerate(all_chunks):
        print(f"Chunk {i + 1}:")
        print(f"Text: {chunk.page_content}")
        print(f"Source: {chunk.metadata['source']}")
        print()
