import os

class PostgresConfig:
    """PostgreSQL database configuration."""
    DB_NAME = "cycling_data"
    URL = f"jdbc:postgresql://pgdatabase:5432/{DB_NAME}"
    USER = os.getenv("POSTGRES_USER", "postgres")
    PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    PROPERTIES = {
        "user": USER,
        "password": PASSWORD,
        "driver": "org.postgresql.Driver"
    }


class CyclingDataAPI:
    """Cycling data API details."""
    ROOT_URL = "https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme"
    YEARS = ["2018", "2019", "2020", "2021", "2022"]
    QUARTERS = ["Q1 (Jan-Mar)", "Q2 spring (Apr-Jun)",
                "Q3 (Jul-Sep)", "Q4 autumn (Oct-Dec)"]
    SUB_NAME = "Central"
    FILE_TYPE = ".csv"
    DB_FIELDS = ["Year", "UnqID", "Date", "Weather", "Time",
                 "Day", "Round", "Dir", "Path", "Mode", "Count"]


class KafkaConfig:
    """Kafka connection details."""
    PRODUCER_PROPERTIES = {
        'bootstrap.servers': "kafka1:9092,kafka2:9093,kafka3:9094",
        'acks': 'all',
        'enable.idempotence': True,
        'transactional.id': 'cycling-producer-transactional-id',
        'retries': 5,
        'max.in.flight.requests.per.connection': 1,
        'queue.buffering.max.messages': 1000000,  # Increase buffer size
        'queue.buffering.max.kbytes': 2097152,    # Increase buffer size in KB
        'batch.num.messages': 20000,              # Increase batch size
        'linger.ms': 50                           # Increase linger time
    }
    ADMIN_PROPERTIES = {
        'bootstrap.servers': "kafka1:9092,kafka2:9093,kafka3:9094"
    }
    TOPIC = ["Central", "Inner", "Outer", "Cycleways"]
    NUM_PARTITIONS = 8
    REPLICATION_FACTOR = 3

# Save ETag to a file
PATH_LAST_PROCESSED = "data/processed/last_processed.json"
