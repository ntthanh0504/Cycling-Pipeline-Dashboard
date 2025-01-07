import json
import csv
import requests
import logging
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from typing import List
from io import StringIO
from src.config.constants import PATH_LAST_PROCESSED, CyclingDataAPI, KafkaConfig

logging.basicConfig(level=logging.INFO)


class CyclingProducer:
    def __init__(
        self,
        producer_properties: dict = KafkaConfig.PRODUCER_PROPERTIES,
        admin_properties: dict = KafkaConfig.ADMIN_PROPERTIES,
    ):
        """
        Initialize the Kafka producer and admin client.
        """
        # Initializing the Producer
        self.producer = Producer(producer_properties)
        self.producer.init_transactions()

        # Create the AdminClient using the correct property
        self.admin = AdminClient(admin_properties)

    def create_topic(self, topic: str):
        """Create a Kafka topic."""
        try:
            topic_list = [
                NewTopic(
                    topic,
                    num_partitions=KafkaConfig.NUM_PARTITIONS,
                    replication_factor=KafkaConfig.REPLICATION_FACTOR,
                )
            ]
            self.admin.create_topics(topic_list, validate_only=False)
            logging.info(f"Topic {topic} created successfully.")
        except KafkaException as e:
            logging.error(f"Failed to create topic {topic}: {e}")

    def publish(self, topic: str, records: List[dict]):
        """
        Publish records to a Kafka topic in batches, ensuring transactional integrity.
        """
        if not records:
            logging.info("No records to publish.")
            return

        messages = [json.dumps(record).encode("utf-8") for record in records]

        try:
            # Begin transaction before producing messages
            logging.info("Starting Kafka transaction...")
            self.producer.begin_transaction()

            # Produce messages within the transaction
            for message in messages:
                self.producer.produce(topic, value=message)
                # Flush producer buffer every 10,000 messages (adjust based on your throughput)
                self.producer.poll(0)

            # Commit transaction if all messages are produced successfully
            logging.info("Committing Kafka transaction...")
            self.producer.commit_transaction()
            logging.info(f"Successfully published {len(records)} records to {topic}.")

        except KafkaException as e:
            # Only abort if the transaction was started
            logging.error(f"Error during publishing: {e}")
            try:
                # Check if we can abort the transaction
                logging.info("Aborting Kafka transaction due to an error...")
                self.producer.abort_transaction()
            except KafkaException as abort_exception:
                logging.error(f"Failed to abort transaction: {abort_exception}")

        finally:
            # Ensure that all messages are flushed to Kafka
            self.producer.flush()


def create_topics(topics: List[str] = KafkaConfig.TOPIC):
    producer = CyclingProducer()
    for topic in topics:
        producer.create_topic(topic)
        
def produce_records(topic: str, records: List[dict]):
    producer = CyclingProducer()
    producer.publish(topic, records)


# def main():
#     producer = CyclingProducer()
#     for topic in KafkaConfig.TOPIC:
#         producer.create_topic(topic)
#     producer.read_all_records("data/filenames.txt", KafkaConfig.TOPIC)
