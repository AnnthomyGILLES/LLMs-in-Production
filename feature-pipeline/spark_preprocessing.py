from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe",
                                                                                             "my-topic").load()

# Parse the JSON data
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Process the data (example: count by name)
processed_df = parsed_df.groupBy("name").count()

# Write the output to console (for demonstration)
query = processed_df.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()
