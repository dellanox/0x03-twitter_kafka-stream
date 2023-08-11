# About the twitter data stream program
The scripts work together to set up a data pipeline that includes data ingestion
from Twitter using the Twitter Streaming API, processing and writing the data
using PySpark, and managing Kafka topics using the KafkaAdminClient. Note that
these scripts have specific dependencies, like kafka, tweepy, and findspark,
that need to be installed for the code to run successfully.

The code provided consists of four Python scripts: consumer.py, producer.py,
stream.py, and topic.py. These scripts are used for consuming, producing,
processing, and managing Kafka messages related to different topics.


### Here's a breakdown of each script's functionality:

#### consumer.py:

This script sets up a Kafka consumer to receive messages from the "Politics" topic.
It deserializes the messages from bytes to JSON format and logs their values.
If there is an exception during the process, a warning is logged.

#### producer.py:

This script establishes a connection to Kafka and utilizes the Twitter Streaming
API to retrieve tweets related to "Politics," "Health," and "School" topics.
It sends the received tweets to the corresponding Kafka topics using the Kafka producer.

#### stream.py:

This script uses PySpark to read data from Kafka topics ("Politics," "Health,"
and "School"). It processes the incoming data by extracting specific fields
from the JSON structure, and then writes the processed data to separate S3
buckets based on the topic.

#### topic.py:

This script manages Kafka topics. It uses the KafkaAdminClient to check if topics
exist, and creates newKafka topics for "Politics," "Health," and "School" if
they don't already exist.
It also lists consumer groups and displays cluster information.
There's commented-out code for deleting topics and describing topic configurations.