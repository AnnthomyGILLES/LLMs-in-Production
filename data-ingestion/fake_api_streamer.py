import logging
import time

from faker import Faker

from kafka_producer import KafkaProducerWrapper


class FakeAPIStreamer:
    def __init__(self, kafka_producer, kafka_topic):
        self.kafka_producer = kafka_producer
        self.kafka_topic = kafka_topic
        self.faker = Faker()  # Initialize Faker instance
        logging.info(f"API Streamer initialized. Kafka Topic: {kafka_topic}")

    def generate_fake_data(self):
        """
        Generates fake data using Faker.

        :return: A dictionary representing fake data
        """
        fake_data = {
            'id': self.faker.random_int(min=1, max=1000),
            'name': self.faker.word(),
            'address': self.faker.address(),
            'email': self.faker.email(),
            'phone_number': self.faker.phone_number()
        }
        return fake_data

    def stream_data(self, interval=1):
        """
        Streams fake data and sends it to Kafka.

        :param interval: Time interval between data generations (in seconds)
        """
        while True:
            try:
                data = self.generate_fake_data()
                self.kafka_producer.producer.send(self.kafka_topic, value=data)
                logging.info(f"Sent data to Kafka: {data}")
            except Exception as e:
                logging.error(f"Error streaming data to Kafka: {str(e)}")

            time.sleep(interval)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    producer = KafkaProducerWrapper(['localhost:9093'])
    streamer = FakeAPIStreamer(producer, 'my-topic')
    streamer.stream_data()
