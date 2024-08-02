from typing import Dict, Any

from langchain.text_splitter import RecursiveCharacterTextSplitter


def chunk_section(
        section: Dict[str, Any], chunk_size: int = 500, chunk_overlap: int = 100
):
    text_splitter = RecursiveCharacterTextSplitter(
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
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
    )
    chunks = text_splitter.create_documents(
        texts=[section["text"]], metadatas=[{"source": section["source"]}]
    )
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
        chunks = chunk_section(section, chunk_size=10, chunk_overlap=5)
        all_chunks.extend(chunks)

    # Print the resulting chunks
    for i, chunk in enumerate(all_chunks):
        print(f"Chunk {i + 1}:")
        print(f"Text: {chunk.page_content}")
        print(f"Source: {chunk.metadata['source']}")
        print()
