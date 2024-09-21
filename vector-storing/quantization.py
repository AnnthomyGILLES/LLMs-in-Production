from typing import Optional, Union

from loguru import logger
from qdrant_client import QdrantClient, models


class QdrantQuantizationHandler:
    def __init__(self, url: str = "http://localhost:6333"):
        self.client = QdrantClient(url=url)
        logger.info(f"Initialized QdrantQuantizationHandler with client at {url}")

    def create_collection_with_quantization(
        self,
        collection_name: str,
        vector_size: int,
        distance: models.Distance,
        quantization_config: Optional[
            Union[models.ScalarQuantization, models.ProductQuantization]
        ] = None,
        on_disk: bool = False,
    ):
        vectors_config = models.VectorParams(
            size=vector_size, distance=distance, on_disk=on_disk
        )

        try:
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=vectors_config,
                quantization_config=quantization_config,
            )
            logger.info(
                f"Created collection '{collection_name}' with quantization config: {quantization_config}"
            )
        except Exception as e:
            logger.error(f"Failed to create collection '{collection_name}': {str(e)}")
            raise

    def create_scalar_quantization(
        self,
        collection_name: str,
        vector_size: int,
        distance: models.Distance,
        quantile: float = 0.99,
        always_ram: bool = True,
    ):
        quantization_config = models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(
                type=models.ScalarType.INT8, quantile=quantile, always_ram=always_ram
            )
        )
        self.create_collection_with_quantization(
            collection_name, vector_size, distance, quantization_config
        )

    def create_product_quantization(
        self,
        collection_name: str,
        vector_size: int,
        distance: models.Distance,
        compression: models.CompressionRatio = models.CompressionRatio.X16,
        always_ram: bool = True,
    ):
        quantization_config = models.ProductQuantization(
            product=models.ProductQuantizationConfig(
                compression=compression, always_ram=always_ram
            )
        )
        self.create_collection_with_quantization(
            collection_name, vector_size, distance, quantization_config
        )

    def update_quantization_config(
        self,
        collection_name: str,
        new_config: Union[models.ScalarQuantization, models.ProductQuantization],
    ):
        try:
            self.client.update_collection(
                collection_name=collection_name, quantization_config=new_config
            )
            logger.info(
                f"Updated quantization config for collection '{collection_name}': {new_config}"
            )
        except Exception as e:
            logger.error(
                f"Failed to update quantization config for collection '{collection_name}': {str(e)}"
            )
            raise

    def get_quantization_config(self, collection_name: str):
        try:
            collection_info = self.client.get_collection(collection_name)
            config = collection_info.config.quantization_config
            logger.info(
                f"Retrieved quantization config for collection '{collection_name}': {config}"
            )
            return config
        except Exception as e:
            logger.error(
                f"Failed to get quantization config for collection '{collection_name}': {str(e)}"
            )
            raise

    def disable_quantization(self, collection_name: str):
        try:
            self.client.update_collection(
                collection_name=collection_name, quantization_config=None
            )
            logger.info(f"Disabled quantization for collection '{collection_name}'")
        except Exception as e:
            logger.error(
                f"Failed to disable quantization for collection '{collection_name}': {str(e)}"
            )
            raise


if __name__ == "__main__":
    handler = QdrantQuantizationHandler()

    # Example usage
    try:
        # Create a collection with scalar quantization
        handler.create_scalar_quantization(
            collection_name="test_scalar",
            vector_size=768,
            distance=models.Distance.COSINE,
        )

        # Create a collection with product quantization
        handler.create_product_quantization(
            collection_name="test_product",
            vector_size=768,
            distance=models.Distance.COSINE,
        )

        # Get quantization config
        scalar_config = handler.get_quantization_config("test_scalar")
        product_config = handler.get_quantization_config("test_product")

        print("Scalar quantization config:", scalar_config)
        print("Product quantization config:", product_config)

        # Update quantization config
        new_config = models.ScalarQuantization(
            scalar=models.ScalarQuantizationConfig(
                type=models.ScalarType.INT8, quantile=0.95, always_ram=False
            )
        )
        handler.update_quantization_config("test_scalar", new_config)

        # Disable quantization
        handler.disable_quantization("test_product")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
