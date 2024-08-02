from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split, size, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    # Define the schema of the incoming data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("post", StringType(), True)
    ])

    # Create SparkSession
    spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

    # Read from Kafka
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:29092").option("subscribe",
                                                                                                     "my-topic").load()

    # Parse JSON data
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Count words per post
    word_count_df = parsed_df.select(
        "id",
        "name",
        "post",
        size(split(col("post"), "\\s+")).alias("word_count")
    )

    # Compute average word count
    avg_word_count = word_count_df.select(avg("word_count").alias("avg_word_count"))

    # Write the output
    query = avg_word_count \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()
