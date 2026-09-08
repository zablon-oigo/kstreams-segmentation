package segmentation;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomerSegmentationApp {

    private static final Logger log = LoggerFactory.getLogger(CustomerSegmentationApp.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";

    private static final String SCHEMA_REGISTRY_URL =  "http://localhost:8081";

    private static final String INPUT_TOPIC = "orders";

    private static final String OUTPUT_TOPIC =  "customer-segments";

    private static final String APPLICATION_ID = "customer-segmentation-app";

    private static void createTopics() {

        Properties properties = new Properties();

        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS
        );

        List<NewTopic> requiredTopics = List.of(
                new NewTopic(INPUT_TOPIC, 3, (short) 1),
                new NewTopic(OUTPUT_TOPIC, 3, (short) 1));

        try (AdminClient admin = AdminClient.create(properties)) {

            Set<String> existingTopics = admin.listTopics().names().get();

            List<NewTopic> topicsToCreate =
                    requiredTopics
                            .stream()
                            .filter(topic ->
                                    !existingTopics.contains(
                                            topic.name()
                                    )
                            )
                            .collect(Collectors.toList());

            if (!topicsToCreate.isEmpty()) {

                admin.createTopics(
                        topicsToCreate
                ).all().get();

                log.info(
                        "Created topics: {}",
                        topicsToCreate
                                .stream()
                                .map(NewTopic::name)
                                .collect(Collectors.joining(", "))
                );

            } else {

                log.info(
                        "All required topics already exist"
                );
            }

        } catch (Exception e) {

            throw new RuntimeException("Failed to create Kafka topics", e);
        }
    }
    private static Properties streamsProperties() {

        Properties properties = new Properties();
        properties.put("application.id", APPLICATION_ID);

        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        properties.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        properties.put("auto.offset.reset", "earliest");
        return properties;
    }


    public static void main(String[] args) {

        log.info("Starting Customer Segmentation Application");

        log.info("Kafka brokers: {}", BOOTSTRAP_SERVERS);

        log.info("Schema Registry: {}", SCHEMA_REGISTRY_URL);

        log.info("Input topic: {}", INPUT_TOPIC);

        log.info("Output topic: {}", OUTPUT_TOPIC);

        createTopics();

        Map<String, String> schemaRegistryConfig =Map.of("schema.registry.url", SCHEMA_REGISTRY_URL);

        Topology topology = CustomerSegmentationTopology.build(schemaRegistryConfig);

        log.info("Kafka Streams topology:\n{}", topology.describe());

        KafkaStreams streams =
                new KafkaStreams(
                        topology,
                        streamsProperties()
                );

        streams.setStateListener(
                (newState, oldState) ->
                        log.info(
                                "Streams state transition: {} -> {}",
                                oldState,
                                newState
                        )
        );

        streams.setUncaughtExceptionHandler(
                exception -> {

                    log.error(
                            "Kafka Streams exception",
                            exception
                    );

                    return
                            org.apache.kafka.streams
                                    .errors
                                    .StreamsUncaughtExceptionHandler
                                    .StreamThreadExceptionResponse
                                    .SHUTDOWN_APPLICATION;
                }
        );

        CountDownLatch latch =
                new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {

                    log.info(
                            "Shutting down Kafka Streams..."
                    );

                    streams.close();

                    latch.countDown();
                })
        );


        try {

            log.info( "Starting Kafka Streams...");
            streams.start();
            latch.await();

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            log.warn(
                    "Application interrupted"
            );

        } finally {

            streams.close();

            log.info(
                    "Customer segmentation application stopped"
            );
        }
    }
}
