from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer


def initialize_model():
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device="cpu")


def create_embedding(model, text):
    return model.encode(text).tolist()


def get_embedding_udf(spark):
    model = initialize_model()
    broadcast_model = spark.sparkContext.broadcast(model)

    @udf(returnType=ArrayType(FloatType()))
    def embedding_udf(text):
        return create_embedding(broadcast_model.value, text)

    return embedding_udf


# This main function is just for testing purposes
def main():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("EmbeddingTest").getOrCreate()

    # Get the UDF
    embedding_udf = get_embedding_udf(spark)

    # Create a sample dataframe
    df = spark.createDataFrame([("This is a test sentence",)], ["text"])

    # Apply the UDF
    result = df.withColumn("embedding", embedding_udf("text"))

    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
