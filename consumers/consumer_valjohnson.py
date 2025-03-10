import os
import json
import sqlite3
import random
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.utils_logger import logger
from collections import deque
import threading

# Load environment variables
load_dotenv()

# Store inventory data for each item in a dictionary
inventory_data = {}

# Constants
ALERT_THRESHOLD = 50
time_window = 50

# Getter Functions for .env Variables
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
    cursor.execute('''CREATE TABLE IF NOT EXISTS inventory (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        item_name TEXT,
                        inventory_level INTEGER
                    )''')
    conn.commit()
    conn.close()
    logger.info("SQLite database initialized.")

def save_to_database(timestamp, item_name, inventory_level):
    """Save inventory data to SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO inventory (timestamp, item_name, inventory_level)
                    VALUES (?, ?, ?)''', (timestamp, item_name, inventory_level))
    conn.commit()
    conn.close()
    logger.info(f"Saved to DB: {timestamp} - {item_name} - {inventory_level}")

#####################################
# Kafka Consumer
#####################################
def create_kafka_consumer():
    """Create and return a Kafka consumer."""
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
inventory_levels = deque(maxlen=time_window)

# Store item names for plotting
item_names = []

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
        # Calculate the average inventory level and plot
        avg_inventory = sum(data["inventory_levels"]) / len(data["inventory_levels"]) if data["inventory_levels"] else 0
        ax.plot(range(len(data["inventory_levels"])), data["inventory_levels"], label=f"{item_name} (avg: {avg_inventory:.2f})", color=data["color"])
    
    # Plot a static threshold line
    ax.axhline(ALERT_THRESHOLD, color='red', linestyle='--', label=f"Threshold ({ALERT_THRESHOLD})")
    
    # Relabel chart after clearing it
    ax.set_xlabel("Data Point")
    ax.set_ylabel("Inventory Level")
    ax.set_title("Live Inventory Levels with Averages")
    
    # Add a legend
    ax.legend() 
    
    # Rotate labels for better visibility
    plt.xticks(rotation=45)
    
    # Ensure layout is tight and labels are not cut off
    plt.tight_layout()

def check_inventory_threshold(item_name, inventory_level):
    """Check if inventory level falls below the alert threshold to alert for inventory reordering purposes."""
    if inventory_level < ALERT_THRESHOLD:
        logger.warning(f"ALERT: {item_name} inventory has dropped below {ALERT_THRESHOLD}! Current level: {inventory_level}. Consider ordering more inventory")

#####################################
# Main Consumer Function
#####################################
def consume_messages():
    """Consumes messages from Kafka topic and processes them and checks for inventory threshold."""
    consumer = create_kafka_consumer()
    logger.info("Kafka consumer started. Listening for messages...")

    try:
        for message in consumer:
            data = message.value
            item_name = data["item_name"]
            inventory_level = int(data["inventory_level"])

            # Save data to the database
            save_to_database(None, item_name, inventory_level)  # Timestamp is ignored for DB saving in this case

            # Initialize the data structure for the item if it doesn't exist
            if item_name not in inventory_data:
                inventory_data[item_name] = {
                    "inventory_levels": deque(maxlen=time_window),
                    "color": get_item_color(item_name)  # Assign a unique color
                }

            # Update inventory data for the item
            inventory_data[item_name]["inventory_levels"].append(inventory_level)

            # Check if inventory level has dropped below the threshold
            check_inventory_threshold(item_name, inventory_level)

            logger.info(f"Received message: {message.value}")

    except KeyboardInterrupt:
        logger.warning("User ceased consumer function.")
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

#####################################
# Start Kafka Consumer and Update Plot
#####################################

def start_consumer_and_plot():
    """Start the consumer in a separate thread and update the plot."""
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    # Create animation, set cache_frame_data=False, avoid warning, keep the animation in memory
    ani = animation.FuncAnimation(fig, update_chart, interval=1000, cache_frame_data=False)  # Update every 1 second

    # Keep the animation alive by not exiting the thread prematurely
    plt.show()  # This will keep the plot window open

    # The animation will keep running in the background while the plot is showing

if __name__ == "__main__":
    setup_database()
    start_consumer_and_plot()  # Start both consumer and plot update

