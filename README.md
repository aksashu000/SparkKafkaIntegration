# SparkKafkaIntegration
Sample code to read from and write to a Kafka topic using Spark.

Also, I have showed how to use 'StreamingQueryListener' for capturing streaming metrics.

# Kafka installation and configuration
I have used Kafka version kafka_2.13-3.0.0 and 'IntelliJ IDEA Community Edition 2021.3' IDE for this developing this.

Follow the instructions available here for installation and configuration of Kafka on your local machine: 

https://kafka.apache.org/quickstart

Steps:
1. Download and configure Kafka
2. Create a topic using Kafka command line utility
3. Load the sample data 'student.json' into the created topic using the below command from <kafka_home>/bin directory:
   ./kafka-console-producer.sh --broker-list localhost:9092 --topic student < [complete_path_to]/student.json
4. You may use an alternative way to load data to Kafka topic. Just run the "MainKafkaProducer" program.
5. Run the Spark program to see streaming in action
6. You can load more data to the created topic using step 3 and see them getting processed in real-time