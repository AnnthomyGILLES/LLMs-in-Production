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

## Switch back to the non-root user
#USER 1001

# Set environment variables
ENV PYSPARK_PYTHON=/opt/bitnami/python/bin/python3

# Expose Spark driver and block manager ports
EXPOSE 7001 7002 4040

RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')"


# Run the script when the container launches
#CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "your_script_name.py"]