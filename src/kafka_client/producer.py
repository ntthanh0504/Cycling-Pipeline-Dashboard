import json
import csv
import requests
import logging
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from typing import List
from io import StringIO
from src.constants import PATH_LAST_PROCESSED, CyclingDataAPI, KafkaConfig

logging.basicConfig(level=logging.INFO)


class CyclingProducer:
    def __init__(self, properties: dict = KafkaConfig.PROPERTIES):
        """
        Initialize the Kafka producer and admin client.

        Args:
            properties (dict): Kafka producer properties
        """
        self.producer = KafkaProducer(**properties)
        self.admin = KafkaAdminClient(**properties)
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
        Read records from a CSV file.

        Args:
            file_name (str): File name
            url (str): URL to fetch the CSV file

        Returns:
            List[dict]: List of records
        """
        http_headers = {"If-None-Match": self.etag.get(file_name)} if self.etag.get(file_name) else {}

        try:
            response = requests.get(url, headers=http_headers, timeout=10)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx responses

            # Update ETag from the response
            new_etag = response.headers.get('ETag')
            if new_etag:
                self.etag[file_name] = new_etag
                self.save_etag(file_name)

            # Parse CSV content
            csv_reader = csv.DictReader(StringIO(response.text))
            return list(csv_reader)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 304:
                logging.info("Data has not changed.")
            else:
                logging.error(f"Request failed: {e}")
        except requests.RequestException as e:
            logging.error(f"Request failed: {e}")
        return []

    def create_topic(self, topic: str):
        """Create a Kafka topic."""
        try:
            topic_list = [
                NewTopic(topic, num_partitions=KafkaConfig.NUM_PARTITIONS, replication_factor=KafkaConfig.REPLICATION_FACTOR)]
            self.admin.create_topics(topic_list, validate_only=False)
            logging.info(f"Topic {topic} created successfully.")
        except Exception as e:
            logging.error(f"Failed to create topic {topic}: {e}")

    def publish(self, topic: str, records: List[dict]):
        """
        Publish records to a Kafka topic.

        Args:
            topic (str): Kafka topic to publish to 
            records (List[dict]): List of records to publish
        """
        if not records:
            logging.info("No records to publish.")
            return

        messages = [json.dumps(record).encode("utf-8") for record in records]
        try:
            # Send messages in batches
            for message in messages:
                self.producer.send(topic, message)
            self.producer.flush()
        except Exception as e:
            logging.error(f"Failed to publish records: {e}")
        else:
            logging.info(f"Successfully published {len(records)} records to {topic}.")


def main():
    producer = CyclingProducer()

    for year in CyclingDataAPI.YEARS:
        all_records = []
        for quarter in CyclingDataAPI.QUARTERS:
            file_name = f"{year} {quarter}-{CyclingDataAPI.SUB_NAME}"
            url = f"{CyclingDataAPI.ROOT_URL}/{year} {quarter}-{CyclingDataAPI.SUB_NAME}{CyclingDataAPI.FILE_TYPE}"
            records = producer.read_records(file_name, url)
            all_records.extend(records)
        topic = f"{KafkaConfig.TOPIC}-{year}"
        producer.create_topic(topic)
        producer.publish(topic, all_records)


if __name__ == "__main__":
    main()
