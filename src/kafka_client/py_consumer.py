import json
import logging
from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
from pymongo import MongoClient
from typing import List
from src.config.constants import KafkaConfig, MongoDBConfig
from src.helpers.mongo_helpers import insert_data

logging.basicConfig(level=logging.INFO)


class CyclingConsumer:
    def __init__(self, consumer_properties: dict = KafkaConfig.CONSUMER_PROPERTIES):
        """
        Initialize the Kafka consumer and MongoDB client.
        """
        # Initializing the Consumer
        self.consumer = Consumer(consumer_properties)
        # Setting up Kafka producer for DLQ
        self.producer = Producer(KafkaConfig.PRODUCER_PROPERTIES)

    def subscribe_to_topics(self, topics: List[str]):
        """Subscribe to Kafka topics."""
        self.consumer.subscribe(topics)
        logging.info(f"Subscribed to topics: {topics}")

    def consume_messages(self, batch_size: int = 100, timeout: float = 1.0):
        """Consume messages from Kafka and save them to MongoDB in batches for each topic."""
        message_batch = {
            topic: [] for topic in KafkaConfig.TOPIC
        }  # Create a batch for each topic

        try:
            while True:
                msg = self.consumer.poll(timeout=timeout)

                if msg is None:
                    logging.warning("No new message yet")
                    break  # No new message yet
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info(
                            f"End of partition reached {msg.partition} at offset {msg.offset}"
                        )
                    else:
                        raise KafkaException(msg.error())
                else:
                    val = msg.value().decode("utf-8")
                    headers = msg.headers()
                    topic = msg.topic()

                    try:
                        # Process the message based on topic
                        # logging.info(f"Received message from topic '{topic}': {val}")
                        message_batch[topic].append(val)
                    except Exception as e:
                        logging.error(f"Error processing message: {e}")
                        dlq_retry = headers.get("dlq_retry", [b"0"])[0].decode(
                            "utf-8"
                        )  # Default to '0' if header not present
                        dlq_retry_max = headers.get("dlq_retry_max", [b"3"])[0].decode(
                            "utf-8"
                        )  # Default to '3' if header not present
                        self.send_to_dlq(
                            msg, str(e), dlq_retry, dlq_retry_max
                        )  # Send the failed message to DLQ
                        continue  # Skip to the next message

                # Save messages to MongoDB when batch size is reached for each topic
                for topic, batch in message_batch.items():
                    if len(batch) >= batch_size:
                        logging.info(
                            f"Saving {len(batch)} messages from topic '{topic}' to MongoDB."
                        )
                        self.save_to_mongodb(batch, topic)
                        message_batch[
                            topic
                        ].clear()  # Clear the batch for this topic after saving

        except KeyboardInterrupt:
            logging.info("Kafka consumer stopped manually.")
        except Exception as e:
            logging.error(f"Error while consuming messages: {e}")
        finally:
            for topic, batch in message_batch.items():
                # Save any remaining messages for each topic after exiting the loop
                if batch:
                    self.save_to_mongodb(batch, topic)
            self.consumer.close()

    def save_to_mongodb(self, batch: List[str], topic: str):
        """Save a batch of consumed Kafka messages from a specific topic to MongoDB."""
        try:
            # Convert all messages to JSON and prepare for bulk insert
            documents = [json.loads(data) for data in batch]
            if documents:
                insert_data("staging", topic, documents)  # Insert by topic
                logging.info(
                    f"Inserted {len(documents)} documents from topic '{topic}' into MongoDB."
                )
        except Exception as e:
            logging.error(f"Error saving data from topic '{topic}' to MongoDB: {e}")

    def send_to_dlq(self, msg, error_message: str, dlq_retry: str, dlq_retry_max: str):
        """Send a failed message to the Dead Letter Queue (DLQ)."""
        dlq_topic = KafkaConfig.DLQ_TOPIC
        dlq_message = {
            "original_message": msg.value().decode("utf-8"),
            "error_message": error_message,
            "timestamp": msg.timestamp(),
            "partition": msg.partition(),
            "offset": msg.offset(),
        }

        # Constructing headers with retry info
        headers = {"dlq_retry": dlq_retry, "dlq_retry_max": dlq_retry_max}

        try:
            # Produce the error message to the DLQ topic with headers
            self.producer.produce(
                dlq_topic, value=json.dumps(dlq_message), key=msg.key(), headers=headers
            )
            self.producer.flush()  # Ensure the message is sent
            logging.info(f"Sent failed message to DLQ with retry info: {headers}")
        except Exception as e:
            logging.error(f"Failed to send message to DLQ: {e}")


def main():
    consumer = CyclingConsumer()
    consumer.subscribe_to_topics(KafkaConfig.TOPIC)
    consumer.consume_messages(
        batch_size=10000, timeout=5.0
    )  # Adjust batch size for optimal performance


if __name__ == "__main__":
    main()
