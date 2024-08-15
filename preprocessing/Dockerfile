# Use Bitnami Spark image
FROM bitnami/spark:3.5

USER root

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install pip and any needed packages specified in requirements.txt
RUN apt-get update && apt-get install -y python3-pip
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Set environment variables
ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python3
ENV SPARK_MASTER_URL=spark://spark:7077
ENV SPARK_LOCAL_IP=0.0.0.0
ENV SPARK_DRIVER_HOST=preprocessing
ENV SPARK_DRIVER_PORT=7001
ENV SPARK_DRIVER_BINDADDRESS=0.0.0.0
ENV SPARK_BLOCKMANAGER_PORT=7002
ENV SPARK_WORKER_MEMORY=4g
ENV SPARK_WORKER_CORES=2
ENV HF_HOME=/tmp/huggingface_cache

# Expose Spark driver and block manager ports
EXPOSE 7001 7002 4040

# Pre-download the SentenceTransformer model
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"

# Set the command to run on container start
CMD spark-submit \
    --master ${SPARK_MASTER_URL} \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 \
    --conf spark.executor.memory=4g \
    --conf spark.driver.memory=4g \
    --conf spark.executor.cores=2 \
    --conf spark.driver.cores=2 \
    --conf spark.streaming.kafka.maxRatePerPartition=100 \
    /app/src/spark_preprocessing.py