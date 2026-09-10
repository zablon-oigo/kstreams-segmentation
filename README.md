Kstream Customer Segmentation

This project demonstrate how to segment customer based on their order purchases 

Architecture Diagram


Running the Project

The project includes Docker Compose for the local Kafka environment.
```sh
docker compose up -d
```
Check the running containers:
```sh
docker ps
```
Start the Producer
```sh
mvn exec:java -Dexec.mainClass=segmentation.Producer
```
Start the Kafka Streams Application
```sh
mvn exec:java -Dexec.mainClass=segmentation.CustomerSegmentationApp
```