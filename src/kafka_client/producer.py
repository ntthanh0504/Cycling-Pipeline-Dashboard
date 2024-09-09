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
    def __init__(self, producer_properties: dict = KafkaConfig.PRODUCER_PROPERTIES, admin_properties: dict = KafkaConfig.ADMIN_PROPERTIES):
        """
        Initialize the Kafka producer and admin client.
        """
        # Initializing the Producer
        self.producer = Producer(producer_properties)
        self.producer.init_transactions()

        # Create the AdminClient using the correct property
        self.admin = AdminClient(admin_properties)

        # Load ETags for caching
        self.etag = self.load_etag()

    def load_etag(self, path: str = PATH_LAST_PROCESSED):
        """Load the ETag from a file."""
        try:
            with open(path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save_etag(self, file_name: str, path: str = PATH_LAST_PROCESSED):
        """Save the ETag to a file."""
        self.etag[file_name] = self.etag.get(file_name)
        with open(path, "w") as f:
            json.dump(self.etag, f)

    def read_records(self, file_name: str, url: str) -> List[dict]:
        """
        Read records from a CSV file and return as a list of dictionaries.
        """
        http_headers = {"If-None-Match": self.etag.get(file_name)} if self.etag.get(file_name) else {}

        try:
            response = requests.get(url, headers=http_headers, timeout=10)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses

            if response.status_code == 304:
                logging.info(f"No updates for {file_name}.")
                return []

            # Update ETag from the response
            new_etag = response.headers.get('ETag')
            if new_etag:
                self.etag[file_name] = new_etag
                self.save_etag(file_name)

            # Parse CSV content
            csv_reader = csv.DictReader(StringIO(response.text))
            return list(csv_reader)

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTPError for {file_name}: {e}")
        except requests.RequestException as e:
            logging.error(f"RequestException for {file_name}: {e}")
        return []

    def read_all_records(self, file_path: str, topics: List[str]):
        try:
            with open(file_path, "r") as f:
                while True:
                    file_name = f.readline().strip()
                    if not file_name:
                        break
                    url = f"{CyclingDataAPI.ROOT_URL}/{file_name}"
                    records = self.read_records(file_name, url)
                    area = file_name.strip().split(".")[0].split("-")[-1]
                    index = topics.index(area)
                    self.publish(topics[index], records)
        except FileNotFoundError as e:
            logging.error(f"File not found: {e}")
            
    def create_topic(self, topic: str):
        """Create a Kafka topic."""
        try:
            topic_list = [
                NewTopic(topic, num_partitions=KafkaConfig.NUM_PARTITIONS, replication_factor=KafkaConfig.REPLICATION_FACTOR)
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


def main():
    producer = CyclingProducer()
    for topic in KafkaConfig.TOPIC:
        producer.create_topic(topic)
    producer.read_all_records("data/filenames.txt", KafkaConfig.TOPIC)


if __name__ == "__main__":
    main()
