# Import necessary libraries fro tweepy
from json import dumps
import logging
import json
from dotenv import dotenv_values
import tweepy
import kafka

# Set up logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ::%(levelname)s::%(name)s --> %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Define the Kafka server address
# NOTE THAT THIS COULL CHANGE. See readme
server = '35.175.217.207'

# Load configuration values from a .env file
config = dotenv_values(".env")
twitter_bearer_token = config["TWITTER_BEARER_TOKEN"]

# Define the names of topics

topics_name = ['Politics', 'Health', 'School']

# Print the first topic name
print(topics_name[0])

def connect_to_kafka(server):
    try:
        # Create a Kafka producer instance
        producer = kafka.KafkaProducer(
            bootstrap_servers=server,
        )
        return producer

    except kafka.errors.NoBrokersAvailable:
        raise kafka.errors.NoBrokersAvailable(f"No brokers available at the specified url: {server}")

def send_message(producer: kafka.KafkaProducer, topic: str, message: bytes):
    # Send a message to the specified Kafka topic
    producer.send(topic, message)

class TwitterStreamingClient(tweepy.StreamingClient):

    def __init__(self, bearer_token, kafka_producer, kafka_topic, *args, **kwargs):
        super().__init__(bearer_token, *args, **kwargs)
        self.producer = kafka_producer
        self.topic = kafka_topic

        print(self.topic, self.producer)

    def on_data(self, raw_data):
        # Print and process raw data
        print(type(raw_data), raw_data)

        data = json.loads(raw_data)
        print(data)

        # Send the raw data to the Kafka topic
        send_message(self.producer, self.topic, raw_data)

    def on_connection_error(self):
        # Handle connection error
        print(self.consumer_key)
        logger.info("A connection error occurred")

    def on_request_error(self, status_code):
        # Handle request error
        print(self.consumer_key)
        logger.info(f"An error of status {status_code} occurred")

try:
    # Connect to Kafka server
    producer = connect_to_kafka(server)

    # Set up Twitter streaming clients for different topics
    stream_politics = TwitterStreamingClient(twitter_bearer_token, producer, topics_name[0])
    stream_politics.add_rules(tweepy.StreamRule(topics_name[0]))
    stream_politics.filter()

    stream_health = TwitterStreamingClient(twitter_bearer_token, producer, topics_name[1])
    stream_health.add_rules(tweepy.StreamRule(topics_name[1]))
    stream_health.filter()

    stream_school = TwitterStreamingClient(twitter_bearer_token, producer, topics_name[2])
    stream_school.add_rules(tweepy.StreamRule(topics_name[2]))
    stream_school.filter()

except KeyboardInterrupt:
    # Delete streaming rules if interrupted
    stream_politics.delete_rules(topics_name[0])
    stream_politics.delete_rules(topics_name[1])
    stream_politics.delete_rules(topics_name[2])

except Exception as e:
    # Print any other exceptions that occur
    print(e)
