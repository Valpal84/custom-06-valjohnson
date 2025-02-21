import os
import json
import sqlite3
import matplotlib.pyplot as plt
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

def get_kafka_topic() -> str:
    return os.getenv("INVENTORY_TOPIC", "inventory_topic")

def get_kafka_consumer_group_id() -> str:
    return os.getenv("INVENTORY_CONSUMER_GROUP_ID", "default_group")

def setup_database():
    conn = sqlite3.connect("inventory.db")
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS inventory (
                      timestamp TEXT,
                      item_name TEXT,
                      inventory_level INTEGER)''')
    conn.commit()
    return conn

def save_to_database(conn, timestamp, item_name, inventory_level):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO inventory VALUES (?, ?, ?)", (timestamp, item_name, inventory_level))
    conn.commit()

def plot_inventory_levels(inventory_data):
    plt.figure(figsize=(10, 6))
    for item, counts in inventory_data.items():
        plt.plot(counts, label=item)
    plt.xlabel('Time')
    plt.ylabel('Inventory Level')
    plt.title('Inventory Levels Over Time')
    plt.legend()
    plt.tight_layout()
    plt.show()

def process_message(message, rolling_window, inventory_data, conn):
    try:
        data = json.loads(message)
        timestamp = data.get("timestamp")
        item_name = data.get("item_name")
        inventory_level = data.get("inventory_level")

        if None in (timestamp, item_name, inventory_level):
            logger.error(f"Invalid message format: {message}")
            return
        
        rolling_window.append(inventory_level)
        save_to_database(conn, timestamp, item_name, inventory_level)

        if item_name not in inventory_data:
            inventory_data[item_name] = []
        inventory_data[item_name].append(inventory_level)
    
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    logger.info("Starting Kafka consumer.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)
    conn = setup_database()
    rolling_window = deque(maxlen=10)
    inventory_data = {}

    try:
        for message in consumer:
            process_message(message.value, rolling_window, inventory_data, conn)
            if len(rolling_window) % 10 == 0:
                plot_inventory_levels(inventory_data)
    except KeyboardInterrupt:
        logger.warning("Consumer stopped by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        conn.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()
