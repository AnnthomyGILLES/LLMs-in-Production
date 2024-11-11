from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)

from src.chunking import chunk_text
from src.embeddings import get_embedding_udf

if __name__ == "__main__":
    # Define the schema of the incoming data
    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("post", StringType(), True),
        ]
    )

    # Create SparkSession
    spark = (
        SparkSession.builder.appName("KafkaSparkStreaming")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        .config("spark.streaming.kafka.consumer.poll.ms", "60000")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        # Create UDFs
        chunk_text_udf = udf(lambda x: chunk_text(x), ArrayType(StringType()))
        create_embedding_udf = get_embedding_udf(spark)  # Get the embedding UDF

        # Read from Kafka
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "redpanda:29092")
            .option("subscribe", "my-topic")
            .load()
        )

        # Parse JSON from Kafka
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Apply chunking to the "post" field
        chunked_df = parsed_df.withColumn("chunks", chunk_text_udf(col("post")))

        # Explode the chunks and create embeddings
        embedded_df = chunked_df.selectExpr(
            "id",
            "name",
            "address",
            "email",
            "phone_number",
            "post",
            "explode(chunks) as chunk",
        ).withColumn("embedding", create_embedding_udf(col("chunk")))

        # Prepare metadata
        output_df = embedded_df.select(
            col("id"),
            to_json(
                struct(col("name"), col("address"), col("email"), col("phone_number"))
            ).alias("metadata"),
            col("chunk").alias("post"),
            col("embedding"),
        )

        # Convert the DataFrame to JSON format
        json_df = output_df.select(to_json(struct("*")).alias("value"))

        # Write to Kafka
        query = (
            json_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", "redpanda:29092")
            .option("topic", "output-spark-topic")
            .option("checkpointLocation", "/tmp/checkpoint")
            .start()
        )

        query.awaitTermination()
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        if "query" in locals():
            query.stop()
        spark.stop()
