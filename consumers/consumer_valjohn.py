# consumer_valjohnson.py

# Consume json messages from a Kafka topic and process them.

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
import matplotlib.pyplot as plt
from collections import deque
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
    topic = os.getenv("CRAFT_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("CRAFT_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_stall_threshold() -> float:
    """Fetch stall threshold from environment or use default."""
    count_variation = float(os.getenv("CRAFT_STALL_THRESHOLD_", 2))
    logger.info(f"Max count variation before stall: {count_variation}")
    return count_variation

def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("CRAFT_ROLLING_WINDOW_SIZE", 8))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

#####################################
# Define a function to detect a stall
#####################################

def detect_stall(rolling_window_deque: deque) -> bool:
    """
    Detect a count stall based on the rolling window.

    Args:
        rolling_window_deque (deque): Rolling window of inventory levels.

    Returns:
        bool: True if a stall is detected, False otherwise.
    """
    # Get the rolling window size from the environment or use a default
    WINDOW_SIZE: int = get_rolling_window_size()

    # If the rolling window has fewer items than the defined window size, we can't detect a stall yet
    if len(rolling_window_deque) < WINDOW_SIZE:
        # We don't have a full deque yet, keep reading until it's full
        logger.debug(
            f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE}."
        )
        return False

    # Once the deque is full, calculate the range between the highest and lowest inventory levels
    count_range = max(rolling_window_deque) - min(rolling_window_deque)

    # If the difference (range) is less than or equal to the threshold, we have a stall
    is_stalled: bool = count_range <= get_stall_threshold()
    logger.debug(f"Count range: {count_range}. Stalled: {is_stalled}")
    
    return is_stalled

#####################################
# Function to process a single message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON-transferred CSV message and check for stalls.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of inventory readings.
        window_size (int): Size of the rolling window.
    """
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

        # Check for reorder alert
        if count < 25:
            logger.warning(f"LOW STOCK ALERT: '{craft_supply}' count is {count}. Reorder recommended!")
            send_sms_alert(craft_supply, count)

        # Check for a stall
        if detect_stall(rolling_window):
            logger.info(
                f"STALL DETECTED at {craft_supply}: count stable at {count} over last {window_size} readings."
            )

        # Plot the inventory levels for visualization (optional)
        plot_inventory_levels(rolling_window)

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

#####################################
# Function to send an SMS alert when inventory is low
#####################################

def send_sms_alert(item_name: str, count: int) -> None:
    """Send an SMS alert when inventory falls below a threshold."""
    from twilio.rest import Client

    # Get the Twilio credentials from the environment
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    from_phone = os.getenv("TWILIO_PHONE_NUMBER")
    to_phone = os.getenv("TO_PHONE_NUMBER")

    # Initialize Twilio client
    client = Client(account_sid, auth_token)

    # Prepare the SMS content
    message = f"ALERT: Low stock of '{item_name}'. Inventory is at {count}. Reorder immediately!"

    # Send the SMS
    client.messages.create(
        body=message,
        from_=from_phone,
        to=to_phone
    )

    logger.info(f"Sent SMS alert for {item_name} with {count} units.")

#####################################
# Function to plot inventory levels (optional)
#####################################

def plot_inventory_levels(rolling_window: deque) -> None:
    """Plot the inventory levels over time."""
    plt.figure(figsize=(10, 6))
    plt.plot(rolling_window, label="Inventory Level")
    plt.xlabel("Time")
    plt.ylabel("Inventory Level")
    plt.title("Inventory Levels Over Time")
    plt.legend()
    plt.show()

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("User ceased consumer function.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
