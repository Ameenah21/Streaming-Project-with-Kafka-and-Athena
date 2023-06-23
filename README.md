# Data Engineering Streaming Project with Kafka and Athena
This repository contains a data engineering streaming project that utilizes Apache Kafka for real-time data processing. The project involves setting up Kafka on an EC2 instance and creating a data catalog in Athena for efficient querying of the processed data.

Project Overview
The goal of this project is to showcase a basic data streaming pipeline using Kafka. Here's a high-level overview of the project flow:

Data Producer: The producer.ipynb notebook sets up a Kafka producer that streams data to the Kafka topic named 'stockprices'. It uses a Pandas DataFrame to generate sample stock data and sends it as JSON messages to the Kafka broker. The producer sends both an initial message and continuously streams random stock data.

Data Consumer: The consumer.ipynb notebook sets up a Kafka consumer that subscribes to the 'stockprices' topic and retrieves the streamed data. It deserializes the messages and writes them as JSON files to an S3 bucket using the S3FileSystem library. Each received message is written as a separate file, named with a unique identifier.


Data Storage: The data is stored in a data lake or data warehouse. In this project, we utilize Amazon S3 as the storage layer, where the transformed data is persisted in a structured format.

Data Cataloging: Athena, a serverless interactive query service provided by AWS, is used to create a data catalog for the stored data in S3. This enables fast and ad-hoc querying of the transformed data using standard SQL syntax.

# Repository Structure
The repository is structured as follows:

kafka-setup/: Contains the necessary scripts and configurations to set up Kafka on an EC2 instance. This includes installation instructions, configuration files, and example producers/consumers for testing.

stream-processing/: Contains the code for data transformation using Kafka Streams or Spark Streaming. Depending on your preference and requirements, you can choose either Kafka Streams or Spark Streaming as the stream processing framework.

athena-cataloging/: Provides scripts and instructions to create a data catalog in Athena. It includes the necessary SQL statements to define tables and partitions based on the transformed data in S3.

README.md: The main README file providing an overview of the project and instructions for getting started.

# Getting Started
To run this project, follow these steps:

Clone this repository to your local machine.

Navigate to the script.sh directory and follow the instructions provided in the README file to set up Kafka on an EC2 instance.

Ensure that you have Kafka and the required Python libraries installed.

Open the producer.ipynb notebook in Jupyter Notebook or JupyterLab. Update the bootstrap_servers parameter in the KafkaProducer initialization with the appropriate IP address and port of your Kafka broker in this case your ec2 instance. The script creates the topic and also starts the server.

Execute the cells to start producing and streaming data to Kafka.

Open the consumer.ipynb notebook in Jupyter Notebook or JupyterLab. Similarly, update the bootstrap_servers parameter in the KafkaConsumer initialization with the correct IP address and port of your Kafka broker. Execute the cells to consume the data, deserialize it, and write it to JSON files in the S3 bucket.

After the data transformation step is completed, proceed to the athena-cataloging/ directory. Follow the instructions provided in the README file to create a data catalog in Athena and define tables for querying the transformed data.

Finally, you can explore and analyze the transformed data by executing SQL queries using Athena's interactive query editor or any other SQL client of your choice.

# Dependencies
The project relies on the following technologies and frameworks:

pandas: A powerful data manipulation library in Python.
kafka-python: A Python client for Apache Kafka.
json: A library for working with JSON data.
time: A Python module for time-related functions.
Amazon S3: A scalable object storage service provided by AWS.
s3fs: A Python library for accessing files in S3.
Amazon Athena: A serverless interactive query service for analyzing data in S3.
Make sure to install and configure these dependencies before running the project.




