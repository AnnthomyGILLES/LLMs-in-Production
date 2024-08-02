from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType

from transformations.chunking import chunk_section
from transformations.embeddings import create_embedding

if __name__ == '__main__':
    # Define the schema of the incoming data
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("post", StringType(), True)
    ])

    # Create SparkSession
    spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

    # Register UDFs
    chunk_udf = udf(lambda text, source: [{"text": chunk.page_content, "source": chunk.metadata["source"]}
                                          for chunk in chunk_section({"text": text, "source": source})],
                    ArrayType(StructType([
                        StructField("text", StringType(), True),
                        StructField("source", StringType(), True)
                    ])))

    embedding_udf = udf(create_embedding, ArrayType(FloatType()))

    # Read from Kafka
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:29092").option("subscribe",
                                                                                                     "my-topic").load()

    # Parse JSON data
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Apply chunking
    chunked_df = parsed_df.withColumn("chunks", chunk_udf(col("post"), col("id").cast("string")))

    # Explode chunks and create embeddings
    processed_df = chunked_df.select("id", "name", "chunks.*").withColumn("embedding", embedding_udf(col("text")))

    # Write the output
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()
