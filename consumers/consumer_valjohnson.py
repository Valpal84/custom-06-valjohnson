"""
kafka_consumer_case.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
from collections import defaultdict
import nltk
import matplotlib.pyplot as plt
import threading
import time

# import external modules
from kafka import KafkaConsumer
from nltk.sentiment import SentimentIntensityAnalyzer

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_case import init_db, insert_message

nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()

author_sentiments = defaultdict(list)

time_series = []
eve_sentiments =[]
other_sentiments = []


#####################################
# Function to process a single message
# #####################################

def analyze_sentiment(text):
    """Perform sentiment analysis on the given text."""
    sentiment_score = sia.polarity_scores(text)["compound"]
    return sentiment_score

def process_message(message: dict) -> None:
    """
    Process and transform a single JSON message.
    Performs sentiment analysis and tracks author sentiment scores.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")

    try:
        text = message.get("message", "")
        if not text:  # Ensure message text exists
            raise ValueError("Empty message text received.")

        sentiment_score = analyze_sentiment(text)  # Get sentiment score

        processed_message = {
            "message": text,
            "author": message.get("author", "Unknown"),  # Default if missing
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": sentiment_score,
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }

        author_sentiments[processed_message["author"].lower()].append(sentiment_score)
        logger.info(f"Processed message: {processed_message}")
        compare_eve_sentiment()
        return processed_message

    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None
    
def plot_sentiment_comparison():
    """Continuously update a live line chart comparing Eve's sentiment to others."""
    plt.ion()  # Turn on interactive mode
    fig, ax = plt.subplots()
    
    while True:
        ax.clear()
        ax.plot(time_series, eve_sentiments, label="Eve", marker="^", linestyle="-", color="blue")
        ax.plot(time_series, other_sentiments, label="Others", marker="o", linestyle="-", color="green")

        ax.set_title("Sentiment Comparison: Eve vs Others")
        ax.set_xlabel("Time Step")
        ax.set_ylabel("Average Sentiment Score")
        ax.legend()
        ax.grid(True)
        
        plt.draw()
        plt.pause(2)  # Refresh every 2 seconds

def compare_eve_sentiment():
    """Compare Eve's sentiment scores to the average sentiment of all other authors."""
    eve_scores = author_sentiments.get("eve", [])
    other_scores = [
        score for author, scores in author_sentiments.items() if author != "eve" for score in scores
    ]

    avg_eve = sum(eve_scores) / len(eve_scores) if eve_scores else None
    avg_other = sum(other_scores) / len(other_scores) if other_scores else None

    if avg_eve is not None and avg_other is not None:
        logger.info(f"Eve's avg sentiment: {avg_eve:.3f}, Others' avg sentiment: {avg_other:.3f}")
        time_series.append(len(time_series) + 1)
        eve_sentiments.append(avg_eve)
        other_sentiments.append(avg_other)
    elif avg_eve is not None:
        logger.info(f"Eve's avg sentiment: {avg_eve:.3f}, but no other authors' messages to compare.")
    elif avg_other is not None:
        logger.info(f"No messages from Eve yet. Others' avg sentiment: {avg_other:.3f}")
    else:
        logger.info("No messages received from any authors yet.")


#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        # consumer is a KafkaConsumer
        # message is a kafka.consumer.fetcher.ConsumerRecord
        # message.value is a Python dictionary
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, sql_path)

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    plot_thread =threading.Thread(target=plot_sentiment_comparison, daemon=True)
    plot_thread.start()

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
