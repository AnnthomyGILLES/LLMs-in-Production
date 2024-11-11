import time
import uuid

import requests
from loguru import logger

from common.kafka_utils.kafka_producer import KafkaProducerWrapper


class RandomUserStreamer:
    def __init__(self, kafka_producer, kafka_topic):
        self.api_url = "https://randomuser.me/api/"
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic
        logger.info(f"Random User Streamer initialized. API URL: {self.api_url}")

    def get_data(self):
        """Fetch data from Random User API"""
        response = requests.get(self.api_url)
        if response.status_code == 200:
            return response.json()["results"][0]
        raise Exception(f"API request failed with status code: {response.status_code}")

    def format_data(self, raw_data):
        """Format the raw API data into desired structure"""
        location = raw_data["location"]
        return {
            "id": str(uuid.uuid4()),
            "first_name": raw_data["name"]["first"],
            "last_name": raw_data["name"]["last"],
            "gender": raw_data["gender"],
            "address": f"{str(location['street']['number'])} {location['street']['name']}, "
            f"{location['city']}, {location['state']}, {location['country']}",
            "post_code": location["postcode"],
            "email": raw_data["email"],
            "username": raw_data["login"]["username"],
            "dob": raw_data["dob"]["date"],
            "registered_date": raw_data["registered"]["date"],
            "phone": raw_data["phone"],
            "picture": raw_data["picture"]["medium"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    def stream_data(self, duration=60, interval=1):
        """
        Streams data from the Random User API and sends it to Kafka.

        :param duration: How long to run the streamer (in seconds)
        :param interval: Time interval between API calls (in seconds)
        """
        start_time = time.time()

        while True:
            # Check if we've exceeded the duration
            if duration and time.time() > start_time + duration:
                logger.info("Streaming duration completed")
                break

            try:
                # Get and format data
                raw_data = self.get_data()
                formatted_data = self.format_data(raw_data)

                # Send to Kafka
                self.kafka_producer.producer.send(
                    self.kafka_topic, value=formatted_data
                )
                logger.info(f"Sent data to Kafka: {formatted_data['id']}")

            except Exception as e:
                logger.error(f"Error streaming data from API: {str(e)}")

            time.sleep(interval)


if __name__ == "__main__":
    producer = KafkaProducerWrapper(
        bootstrap_servers=["localhost:9093"], topic="my-topic"
    )
    streamer = RandomUserStreamer(producer, "users_created")
    try:
        streamer.stream_data(duration=60)
    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user")
    finally:
        producer.close()
        logger.info("Kafka producer closed")
