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
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    PROPERTIES = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
    }
    TOPIC = "cycling-pipeline"
    NUM_PARTITIONS = 2
    REPLICATION_FACTOR = 1

# Save ETag to a file
PATH_LAST_PROCESSED = "data/last_processed.json"


