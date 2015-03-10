# KafkaTestDataProducer
Produces random messages and sends to kafka.

Usage: java -jar <jarname> <topic_name> <brokerIds> <hasMultiplePartitions> <# of Messages> <Start index of Message>

Example: java -jar KafkaProducer-0.0.1-SNAPSHOT-jar-with-dependencies.jar test localhost:9092 true 200000 1
