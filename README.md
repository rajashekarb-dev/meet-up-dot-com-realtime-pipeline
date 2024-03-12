# Meetup.com Realtime Data Pipeline
 
## Prerequisites:
* Scala (2.12.6)
* Python (3.9)
* Apache Kafka (2.8.0)
* Apache Spark (3.0.2)
* Mongo DB (4.4.3)
* MySQL (8.0..25)

## Run:
* Start Kafka zookeeper and server
* Start Mysql and MongoDB
* Navigate to Python/src/producers folder and run kafka_producer.py to push messages to Apache Kafka topic
* Navigate to Scala/src/main/scala folder and run spark_stream_processing_app to start consuming from the Apacha Kafka topic
* After Spark app deployed to cluster it will start storing RAW JSON objects Dataframes to Mongo DB and aggregated data to MySQL table in batch mode

