# Import necessary packages
import os
import json
import sqlite3
import random
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv
from utils.utils_logger import logger
from collections import deque

# Load environment variables
load_dotenv()

time_window = 50
inventory_data = {}

# Store inventory data for each item in a dictionary
inventory_data = {}

# Enable interactive mode for Matplotlib (not used here anymore, handling animation manually)
#plt.ion()

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

fig, ax = plt.subplots()

# Assign colors dynamically
def get_item_color(item_name):
    """Assign a random color to each item for unique line on the plot"""
    random.seed(hash(item_name))
    return f'#{random.randint(0, 0xFFFFFF) :06x}'  # Random hex color codes

def update_chart(frame):
    """Update the inventory level chart dynamically."""
    ax.clear()  # Clears the previous chart
    
    # Plot a line for each item
    for item_name, data in inventory_data.items():
        ax.plot(data["timestamps"], data["inventory_levels"], label=item_name, color=data["color"])
    
    ax.set_xlabel("Time")
    ax.set_ylabel("Inventory Level")
    ax.set_title("Live Inventory Levels")
    ax.legend()  # Update the legend with the item names
    plt.xticks(rotation=45)  # Rotate labels for better visibility
    plt.tight_layout()  # Ensure layout is tight and labels are not cut off

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
            data = message.value
            timestamp = data["timestamp"]
            item_name = data["item_name"]
            inventory_level = int(data["inventory_level"])

            # Save data to the database
            save_to_database(timestamp, item_name, inventory_level)

            # Initialize the data structure for the item if it doesn't exist
            if item_name not in inventory_data:
                inventory_data[item_name] = {
                    "timestamps": deque(maxlen=time_window),
                    "inventory_levels": deque(maxlen=time_window),
                    "color": get_item_color(item_name)  # Assign a unique color
                }

            # Update inventory data for the item
            inventory_data[item_name]["timestamps"].append(datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S"))
            inventory_data[item_name]["inventory_levels"].append(inventory_level)

            logger.info(f"Received message: {message.value}")

    except KeyboardInterrupt:
        logger.warning("User ceased consumer function.")
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

def update_plot():
    """Update the plot in an interactive loop."""
    ani = animation.FuncAnimation(fig, update_chart, interval=1000)  # Create the animation object
    plt.show()  # Ensure the plot window remains open
    return ani  # Return the animation object to keep it alive

if __name__ == "__main__":
    setup_database()

    # Start the consumer in the main loop to avoid blocking the UI
    consume_messages()

    # Start updating the plot
    update_plot()  # Start updating the plot

