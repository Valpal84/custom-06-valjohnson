# Import packages from Python Standard Library
import os
import sys
import time  # control message intervals
import pathlib  # work with file paths
import csv  # handle CSV data
import json  # work with JSON data
from datetime import datetime  # work with timestamps

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_logger import logger
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource


#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("INVENTORY_TOPIC", "inventory_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("INVENTORY_INTERVAL_SECONDS", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE = DATA_FOLDER.joinpath("inventory_data.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Kafka Admin Functions
#####################################

def create_kafka_topic(topic: str):
    """Create Kafka topic if it does not exist."""
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id="your_client_id"
    )
    # Check if the topic already exists
    existing_topics = admin_client.list_topics()
    if topic not in existing_topics:
        # Create the new topic
        new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        logger.info(f"Topic '{topic}' created.")
    else:
        logger.info(f"Topic '{topic}' already exists.")
    admin_client.close()

#####################################
# Message Generator
#####################################

def generate_messages(file_path: pathlib.Path):
    """
    Read from a csv file and yield records one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        str: CSV row formatted as a string.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {DATA_FILE}")
            with open(DATA_FILE, "r") as csv_file:
                logger.info(f"Reading data from file: {DATA_FILE}")

                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    row = {key.strip('"'): value for key, value in row.items()}
                    # Ensure required fields are present
                    if "timestamp" not in row or "Item name" not in row or "Inventory level" not in row:
                        logger.error(f"Missing required column in row: {row}")
                        continue

                    message = {
                        "timestamp": row["timestamp"],
                        "item_name": row["Item name"],
                        "inventory_level": int(row["Inventory level"]),
                    }

                    logger.debug(f"Generated message: {message}")
                    yield message

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Create Kafka Producer
#####################################

def create_kafka_producer(value_serializer):
    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=value_serializer
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


#####################################
# Define main function for this module.
#####################################

def main():
    """
    Main entry point for the producer.

    - Reads the Kafka topic name from an environment variable.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams messages to the Kafka topic.
    """

    logger.info("START producer.")
    
    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("User ceased producer function.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
