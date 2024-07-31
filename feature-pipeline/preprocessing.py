from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe",
                                                                                             "my-topic").load()

processed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = processed_df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
