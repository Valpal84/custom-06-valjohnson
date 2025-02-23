# Import necessary packages
import os
import json
import sys
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.utils_logger import logger
from collections import deque

# Load environment variables
load_dotenv()

# Enable interactive mode for Matplotlib
plt.ion()

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
# Inventory levels database setup
#####################################

DB_FILE = "inventorylevels.db"

def setup_database():
    """Initialize SQLite database and create table if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            item_name TEXT,
            inventory_level INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("SQLite database initialized.")

def save_to_database(timestamp, item_name, inventory_level):
    """Save inventory data to SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO inventory (timestamp, item_name, inventory_level)
        VALUES (?, ?, ?)
    ''', (timestamp, item_name, inventory_level))
    conn.commit()
    conn.close()
    logger.info(f"Saved to DB: {timestamp} - {item_name} - {inventory_level}")

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

#####################################
# Live Line Chart Setup
#####################################

# Store the last 50 inventory levels for visualization
time_window = 50
timestamps = deque(maxlen=time_window)
inventory_levels = deque(maxlen=time_window)

fig, ax = plt.subplots()
line, = ax.plot([], [], 'b-', label="Inventory Level")
ax.set_xlabel("Time")
ax.set_ylabel("Inventory Level")
ax.set_title("Live Inventory Levels")
ax.legend()

def update_chart(frame):
    """Update the inventory level chart dynamically."""
    ax.clear()
    ax.plot(timestamps, inventory_levels, 'b-', label="Inventory Level")
    ax.set_xlabel("Time")
    ax.set_ylabel("Inventory Level")
    ax.set_title("Live Inventory Levels")
    ax.legend()

#####################################
# Main Consumer Function
#####################################

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
    setup_database()
    plt.show()
