package segmentation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.datafaker.Faker;
import segmentation.avro.Order;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:29092";

    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static final String TOPIC = "orders";

    private static final int CUSTOMER_COUNT = 100;

    private static final long SEND_INTERVAL_MS = 1000;


    private static Properties buildProducerProperties() {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        return props;
    }

    private static List<String> generateCustomers(Faker faker) {

        List<String> customerIds = new ArrayList<>();

        for (int i = 0; i < CUSTOMER_COUNT; i++) {

            String customerId =
                    "cust-" + faker.internet().uuid();

            customerIds.add(customerId);
        }

        return customerIds;
    }

    private static Order generateOrder(
            Faker faker,
            Random random,
            List<String> customerIds
    ) {


        String customerId =
                customerIds.get(
                        random.nextInt(customerIds.size())
                );

        String orderId = "ord-" + UUID.randomUUID();

        double amount = Double.parseDouble(faker.commerce().price(10, 500));

        String category = faker.commerce().department();

        long timestamp = System.currentTimeMillis();

        return new Order(
                orderId,
                customerId,
                amount,
                timestamp,
                category
        );
    }

    public static void main(String[] args) {

        Faker faker = new Faker();

        Random random = new Random();

        List<String> customerIds = generateCustomers(faker);

        log.info("Generated {} customers", customerIds.size());

        log.info("Starting order producer...");

        log.info("Kafka brokers: {}", BOOTSTRAP_SERVERS);

        log.info("Schema Registry: {}", SCHEMA_REGISTRY_URL);

        log.info("Topic: {}", TOPIC);

        AtomicBoolean running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    log.info(
                            "Shutting down producer..."
                    );
                    running.set(false);
                })
        );

        Properties props = buildProducerProperties();

        try (
                KafkaProducer<String, Order> producer = new KafkaProducer<>(props)
        ) {

            while (running.get()) {

                Order order = generateOrder(
                                faker,
                                random,
                                customerIds
                        );

                ProducerRecord<String, Order> record =
                        new ProducerRecord<>(
                                TOPIC,
                                order.getCustomerId(),
                                order
                        );

                producer.send(
                        record,
                        (metadata, exception) -> {

                            if (exception != null) {

                                log.error(
                                        "Failed to send order",
                                        exception
                                );

                            } else {

                                log.info(
                                        "Sent order: customer={}, amount=${}, category={}, partition={}, offset={}",
                                        order.getCustomerId(),
                                        order.getAmount(),
                                        order.getCategory(),
                                        metadata.partition(),
                                        metadata.offset()
                                );
                            }
                        }
                );

                try {

                    Thread.sleep(SEND_INTERVAL_MS);

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

                    log.info(
                            "Producer interrupted"
                    );

                    break;
                }
            }

        } finally {

            log.info(
                    "Producer stopped"
            );
        }
    }
}

