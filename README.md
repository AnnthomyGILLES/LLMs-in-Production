# Real-Time Retrieval-Augmented Generation (RAG) Project

## Welcome!

Hi there! 👋

Thanks for stopping by to check out my project. This is something I've been working on during my spare time, combining my passion for data engineering, machine learning, and cutting-edge technology into a real-time Retrieval-Augmented Generation (RAG) system. The project is designed to handle everything from data crawling to generating intelligent responses, all split into manageable microservices.

## Project Overview

I’ve broken this project down into several microservices to keep things organized and scalable. Here’s a quick rundown of what each part does:

### 1. **Data Crawling**
   - **What it does:** This microservice goes out and fetches data from sources like LinkedIn, Wikipedia, and other websites. It’s designed to continuously bring in fresh data for the system.
   - **Tech involved:** Python, BeautifulSoup, Scrapy, Selenium, API integrations.

### 2. **Data Ingestion**
   - **What it does:** This service takes the data from the crawler and stores it in MongoDB. I’ve set up a Change Data Capture (CDC) system that detects any new data and sends it to Kafka for processing.
   - **Tech involved:** MongoDB, Kafka, Debezium.

### 3. **Preprocessing**
   - **What it does:** Here’s where the magic happens! Apache Spark consumes messages from Kafka and transforms the text data, cleaning it up and getting it ready for use. Once done, it sends the processed data back to Kafka.
   - **Tech involved:** Apache Spark, Kafka.

### 4. **Vector Storing**
   - **What it does:** This service takes the processed data and stores it in Qdrant, a specialized vector database. It’s optimized for high-dimensional vector data, which is perfect for what we’re doing here.
   - **Tech involved:** Qdrant, Kafka.

### 5. **RAG Folder**
   - **What it does:** This is the heart of the project. It handles the retrieval and generation tasks, using the vector data to create intelligent responses. I’m implementing the latest techniques in NLP to make this as effective as possible.
   - **Tech involved:** Python, PyTorch/TensorFlow, Transformers, FAISS.

## Getting Started

Everything is containerized using Docker, so setting it up is a breeze. If you want to check it out, just follow these steps:

### Prerequisites

- Make sure you’ve got Docker and Docker Compose installed.
- Some familiarity with microservices and the tech stack used will help, but it’s not required.

### How to Run

1. Clone the project:
   ```bash
   git clone https://github.com/yourusername/real-time-rag.git
   cd real-time-rag
   ```

2. Start it up with Docker Compose:
   ```bash
   docker-compose up --build
   ```

3. Watch as the microservices spin up in this order:
   - Data Crawling
   - Data Ingestion
   - Preprocessing
   - Vector Storing
   - RAG Folder

### Usage

Once everything is up and running, the system will start pulling in data, processing it, and getting it ready for RAG tasks. You can play around with it, tweak it, or just watch it do its thing.

## Let’s Collaborate!

I’m always looking for ways to improve this project. If you have any suggestions—whether it’s new tools, concepts, technical ideas, or just some feedback—I’d love to hear from you. Feel free to reach out to me at **ag@willofv.mozmail.com**.

## Closing Thoughts

This project is a labor of love, reflecting my passion for building scalable, modern systems. I hope it gives you a good sense of my skills and what I enjoy working on. Thanks again for checking it out!
