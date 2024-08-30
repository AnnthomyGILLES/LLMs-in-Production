# Real-Time Retrieval-Augmented Generation (RAG) Project

## Overview

This project is a cutting-edge implementation of a real-time Retrieval-Augmented Generation (RAG) system, split into modular microservices to ensure scalability, maintainability, and ease of development. The system is designed to handle the end-to-end process from data acquisition to the generation of responses, leveraging the latest technologies and best practices.

## Architecture

The project is composed of several key microservices, each responsible for a distinct part of the data processing pipeline:

### 1. **Data Crawling**
   - **Description:** This microservice is responsible for fetching data from various sources such as LinkedIn, Wikipedia, and other web platforms. It ensures a continuous and up-to-date stream of information for further processing.
   - **Technologies:** Python, BeautifulSoup, Scrapy, Selenium, API integrations.

### 2. **Data Ingestion**
   - **Description:** The data ingestion microservice handles the integration of crawled data into MongoDB. A Change Data Capture (CDC) mechanism is implemented to detect new entries in MongoDB, which are then sent to Kafka for further processing.
   - **Technologies:** MongoDB, Kafka, Debezium.

### 3. **Preprocessing**
   - **Description:** This microservice utilizes Apache Spark to consume Kafka messages and perform complex transformations and preprocessing on the textual data. The processed data is then published back to Kafka on a separate topic for subsequent consumption.
   - **Technologies:** Apache Spark, Kafka.

### 4. **Vector Storing**
   - **Description:** In this microservice, processed data is consumed from Kafka and stored in Qdrant, a vector database optimized for handling high-dimensional vector data. This enables efficient storage and retrieval of vectorized information.
   - **Technologies:** Qdrant, Kafka.

### 5. **RAG**
   - **Description:** This microservice is the core of the RAG system. It performs retrieval and generation tasks, leveraging the stored vectors to produce intelligent, context-aware responses. The system is built using the latest techniques in Natural Language Processing (NLP) and information retrieval.
   - **Technologies:** Python, PyTorch, Transformers.

## Deployment

Each microservice is containerized using Docker, allowing for seamless deployment and orchestration through Docker Compose. The microservices communicate with each other through well-defined APIs and Kafka topics, ensuring a loosely-coupled and scalable architecture.

### Prerequisites

- Docker and Docker Compose installed on your machine.
- Basic understanding of microservices architecture and the technologies used in this project.

### How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/real-time-rag.git
   cd real-time-rag
   ```

2. Start the services using Docker Compose:
   ```bash
   docker-compose up --build
   ```

3. The microservices will start in the following order:
   - Data Crawling
   - Data Ingestion
   - Preprocessing
   - Vector Storing
   - RAG Folder

### Usage

Once all the services are up and running, the system will automatically begin fetching, processing, and storing data. The RAG Folder microservice will be ready to perform retrieval and generation tasks on request.

## Conclusion

This project showcases my ability to design and implement complex, scalable systems using modern technologies and best practices. Each component is thoughtfully designed to handle specific tasks within a real-time RAG system, demonstrating a deep understanding of data engineering, machine learning, and microservices architecture.
