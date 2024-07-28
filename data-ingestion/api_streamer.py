import logging
import time

import requests

from kafka_producer import KafkaProducerWrapper


class APIStreamer:
    def __init__(self, api_url, kafka_producer, kafka_topic):
        self.api_url = api_url
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic
        logging.info(f"API Streamer initialized. API URL: {api_url}")

    def stream_data(self, interval=1):
        """
        Streams data from the API and sends it to Kafka.

        :param interval: Time interval between API calls (in seconds)
        """
        while True:
            try:
                response = requests.get(self.api_url)
                if response.status_code == 200:
                    data = response.json()
                    self.kafka_producer.producer.send(self.kafka_topic, value=data)
                    logging.info(f"Sent data to Kafka: {data}")
                else:
                    logging.error(f"API request failed with status code: {response.status_code}")
            except Exception as e:
                logging.error(f"Error streaming data from API: {str(e)}")

            time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = KafkaProducerWrapper(['localhost:9092'])
    streamer = APIStreamer('https://api.example.com/data', producer, 'my-topic')
    streamer.stream_data()
