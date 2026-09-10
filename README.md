## Kafka Streams Customer Segmentation

This project demonstrates how to build a real-time customer segmentation pipeline using Apache Kafka Streams, Java, and Avro.

The application consumes customer orders from Kafka, groups them by customerId, maintains running customer statistics in a Kafka Streams state store, and publishes an updated customer segment whenever a new order is processed.

#### Architecture Diagram


#### Running the Project

The project includes Docker Compose for the local Kafka environment.
```sh
docker compose up -d
```
Check the running containers:
```sh
docker ps
```
#### Start the Producer
```sh
mvn exec:java -Dexec.mainClass=segmentation.Producer
```
#### Start the Kafka Streams Application
```sh
mvn exec:java -Dexec.mainClass=segmentation.CustomerSegmentationApp
```

#### Inspecting the Results

Open your browser:
```sh
http://localhost:8087
```
