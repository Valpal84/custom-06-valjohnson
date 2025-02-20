"""
consumer_valjohnson.py

Consume json messages from a Kafka topic and process them.

Example Kafka message format:
{'timestamp': '2024-10-19 03:19:40', 'item_name': 'Badge Toppers', 'inventory_level': 452}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
import matplotlib.pyplot as plt
from twilio.rest import Client

# Use a deque ("deck") - a double-ended queue data structure
# A deque is a good way to monitor a certain number of "most recent" messages
# A deque is a great data structure for time windows (e.g. the last 5 messages)
from collections import deque

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
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

#def get_sms_alert_threshold() -> int:
    threshold = int(os.getenv("SMS_ALERT_THRESHOLD", 25))
    logger.info(f"SMS alert threshold: {threshold}")
    return threshold

#def get_twilio_credentials() -> dict:
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_FROM_NUMBER")
    to_number = os.getenv("TWILIO_TO_NUMBER")
    return {"sid": account_sid, "token": auth_token, "from": from_number, "to": to_number}

# Send SMS Alert
#def send_sms_alert(craft_supply: str, count: int) -> None:
    credentials = get_twilio_credentials()
    client = Client(credentials["sid"], credentials["token"])

    message = client.messages.create(
        body=f"ALERT: '{craft_supply}' inventory is low! Current count: {count}",
        from_=credentials["from"],
        to=credentials["to"],
    )
    logger.info(f"SMS sent: {message.sid}")
    

# Plot Inventory Levels
def plot_inventory_levels(inventory_data: dict) -> None:
    # Plot the inventory levels over time
    plt.figure(figsize=(10, 6))
    for item, counts in inventory_data.items():
        plt.plot(counts, label=item)

    plt.xlabel('Time')
    plt.ylabel('Inventory Level')
    plt.title('Inventory Levels Over Time')
    plt.legend()
    plt.tight_layout()
    plt.show()

# Function to process a single message
def process_message(message: str, rolling_window: deque, inventory_data: dict) -> None:
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        count = data.get("count")
        craft_supply = data.get("craft_supply")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if count is None or craft_supply is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Append the count reading to the rolling window
        rolling_window.append(count)

        # Update inventory data for charting
        if craft_supply not in inventory_data:
            inventory_data[craft_supply] = []
        inventory_data[craft_supply].append(count)

        # Check for low stock and send an SMS alert if necessary
        if count < get_sms_alert_threshold():
            send_sms_alert(craft_supply, count)

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Define main function for this module
#####################################


# Main function for the consumer
def main() -> None:
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Rolling window setup (for stall detection)
    rolling_window = deque(maxlen=8)
    
    # Dictionary to store inventory levels over time
    inventory_data = {}

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, inventory_data)

            # Every 10 messages, plot the inventory levels
            if len(rolling_window) % 10 == 0:
                plot_inventory_levels(inventory_data)

    except KeyboardInterrupt:
        logger.warning("User ceased consumer function.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

if __name__ == "__main__":
    main()


