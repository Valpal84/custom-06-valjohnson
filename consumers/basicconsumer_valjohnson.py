# Import necessary packages
import os
import json
import sys
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("INVENTORY_TOPIC", "inventory_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_bootstrap_servers() -> str:
    """Fetch Kafka bootstrap servers from environment or use default."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Kafka bootstrap servers: {bootstrap_servers}")
    return bootstrap_servers

#####################################
# Kafka Consumer
#####################################

def create_kafka_consumer():
    """
    Create and return a Kafka consumer.
    
    Returns:
        KafkaConsumer: A configured Kafka consumer instance.
    """
    topic = get_kafka_topic()
    bootstrap_servers = get_kafka_bootstrap_servers()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    
    return consumer

def consume_messages():
    """
    Consumes messages from Kafka topic and processes them.
    """
    consumer = create_kafka_consumer()
    logger.info("Kafka consumer started. Listening for messages...")

    try:
        for message in consumer:
            logger.info(f"Received message: {message.value}")
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    consume_messages()
