"""
consumer_valjohnson.py

Consume JSON messages from a Kafka topic and process them.

Example Kafka message format:
{'timestamp': '2024-10-19 03:19:40', 'item_name': 'Badge Toppers', 'inventory_level': 452}
"""

#####################################
# Import Modules
#####################################

import os
import json
import sqlite3
import matplotlib.pyplot as plt
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

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


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("INVENTORY_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# SQLite Database Setup
#####################################


def setup_database():
    """Initialize SQLite database and create table if it doesn't exist."""
    conn = sqlite3.connect("inventory.db")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory (
            timestamp TEXT,
            item_name TEXT,
            inventory_level INTEGER
        )
        """
    )
    conn.commit()
    conn.close()


def save_to_database(timestamp: str, item_name: str, inventory_level: int):
    """Save inventory data to SQLite database."""
    conn = sqlite3.connect("inventory.db")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO inventory (timestamp, item_name, inventory_level) VALUES (?, ?, ?)",
        (timestamp, item_name, inventory_level),
    )
    conn.commit()
    conn.close()


#####################################
# Stall Detection Function
#####################################


def detect_stall(rolling_window_deque: deque) -> bool:
    """
    Detects if inventory levels have remained stable over the rolling window.

    Args:
        rolling_window_deque (deque): Rolling window of inventory levels.

    Returns:
        bool: True if a stall is detected, False otherwise.
    """
    if len(rolling_window_deque) < rolling_window_deque.maxlen:
        # Not enough data to detect a stall
        return False

    count_range = max(rolling_window_deque) - min(rolling_window_deque)
    return count_range == 0  # Stall detected if no variation


#####################################
# Plot Inventory Levels
#####################################


def plot_inventory_levels(inventory_data: dict) -> None:
    """Plot inventory levels over time."""
    plt.figure(figsize=(10, 6))
    for item, counts in inventory_data.items():
        plt.plot(counts, label=item)

    plt.xlabel("Time")
    plt.ylabel("Inventory Level")
    plt.title("Inventory Levels Over Time")
    plt.legend()
    plt.tight_layout()
    plt.show()


#####################################
# Process Kafka Message
#####################################


def process_message(message: str, rolling_window: deque, inventory_data: dict) -> None:
    """
    Process a JSON-transferred CSV message.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of inventory levels.
        inventory_data (dict): Dictionary storing historical inventory levels.
    """
    try:
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)

        timestamp = data.get("timestamp")
        item_name = data.get("item_name")
        inventory_level = data.get("inventory_level")

        logger.info(f"Processed JSON message: {data}")

        # Ensure required fields are present
        if None in (timestamp, item_name, inventory_level):
            logger.error(f"Invalid message format: {message}")
            return

        # Append inventory level to rolling window
        rolling_window.append(inventory_level)

        # Store inventory data in SQLite
        save_to_database(timestamp, item_name, inventory_level)

        # Store inventory levels for plotting
        if item_name not in inventory_data:
            inventory_data[item_name] = []
        inventory_data[item_name].append(inventory_level)

        # Detect inventory stalls
        if detect_stall(rolling_window):
            logger.info(
                f"STALL DETECTED for {item_name}: Inventory stable at {inventory_level} over last {rolling_window.maxlen} readings."
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Main Consumer Function
#####################################


def main() -> None:
    """Main function for Kafka consumer."""
    logger.info("START consumer.")

    # Initialize SQLite database
    setup_database()

    # Load Kafka topic and group ID from environment variables
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create Kafka consumer
    consumer = create_kafka_consumer(topic, group_id)

    # Rolling window setup (for stall detection)
    rolling_window = deque(maxlen=8)

    # Dictionary to store inventory levels over time
    inventory_data = {}

    logger.info(f"Polling messages from topic '{topic}'...")

    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, inventory_data)

            # Plot every 10 messages
            if len(rolling_window) % 10 == 0:
                plot_inventory_levels(inventory_data)

    except KeyboardInterrupt:
        logger.warning("User stopped consumer.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Run the Consumer
#####################################

if __name__ == "__main__":
    main()
