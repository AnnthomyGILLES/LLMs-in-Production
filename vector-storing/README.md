## README: Kafka to Qdrant Integration with Quantization

### Overview
This code integrates Kafka and Qdrant to process real-time streaming data, transforming it into vector embeddings using a `SentenceTransformer` model. The data is then stored in Qdrant, a vector database. This system can utilize different types of quantization, optimizing storage and search performance by reducing the size of vector embeddings.

### Key Features
- **Kafka Integration**: Consumes messages from Kafka topics, transforms them into vector embeddings, and stores them in Qdrant.
- **Qdrant Collection Management**: Automatically creates a collection if it doesn’t exist, using the specified quantization type.
- **Quantization Support**: Three quantization methods are supported:
  - **Scalar Quantization**: Compresses vectors by converting 32-bit floats into 8-bit integers.
  - **Binary Quantization**: Reduces vector components to single bits.
  - **Product Quantization**: Divides vectors into smaller chunks and compresses each part, allowing for a balance between memory efficiency and search accuracy.

### Quantization Explained
Quantization is a method to optimize the storage and retrieval of high-dimensional vectors in Qdrant:
- **Scalar Quantization** reduces the memory footprint of vectors by compressing each component into 8-bit integers, making it ideal for large datasets with minimal accuracy loss.
- **Binary Quantization** turns vector components into 1-bit representations, offering extreme memory savings at the cost of some precision.
- **Product Quantization** chunks vectors into smaller parts, compressing each chunk, achieving a balance between compression and precision, particularly useful for large datasets with a moderate tolerance for reduced accuracy.

### Code Walkthrough
- **QdrantHandler Class**: Handles the connection to Qdrant, ensures collection existence, processes Kafka messages, and inserts vector embeddings into Qdrant.
- **Quantization Types**: Set during collection creation, they control how embeddings are stored to optimize memory and performance.
- **Message Processing**: Real-time data is consumed from Kafka, encoded into vector embeddings using a transformer model, and upserted into Qdrant.

### How to Run
1. Install required dependencies:
   ```bash
   pip install qdrant-client sentence-transformers loguru
   ```
2. Update `kafka_bootstrap_servers`, `kafka_topic`, and `qdrant_url` with your setup.
3. Choose the desired quantization type: `"scalar"`, `"binary"`, or `"product"`.
4. Run the script:
   ```bash
   python main.py
   ```

### Example Usage
```python
qdrant_handler = QdrantHandler(
    qdrant_url="http://localhost:6333",
    collection_name="startups",
    kafka_bootstrap_servers=["localhost:9093"],
    kafka_topic="output-spark-topic",
    quantization_type="scalar"  # Choose from 'scalar', 'binary', 'product'
)
qdrant_handler.process_messages()
```

### License
This project is licensed under the MIT License.
