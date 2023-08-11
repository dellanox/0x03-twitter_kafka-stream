# Import necessary libraries
from kafka import KafkaConsumer
from json import loads
import logging
from dotenv import dotenv_values

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s ::%(levelname)s::%(name)s --> %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

try:
    # Create a Kafka consumer instance
    consumer1 = KafkaConsumer(
        'Politics', bootstrap_servers='35.175.217.207', group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # Subscribe to the 'Politics' topic
    consumer1.subscribe(topics="Politics")

    # Continuously loop to consume and process messages
    for message in consumer1:
        # Log the value of each message received
        logger.info(message.value)
except Exception as e:
    # Log a warning if an exception occurs
    logger.warning(e)
