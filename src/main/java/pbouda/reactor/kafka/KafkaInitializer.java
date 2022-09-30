package pbouda.reactor.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

public class KafkaInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInitializer.class);

    private static final String TOPIC_NAME = "topic-test";

    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))
            .withNetwork(Network.SHARED);

    private static final GenericContainer<?> KAFKA_UI_CONTAINER = new GenericContainer<>(DockerImageName.parse("provectuslabs/kafka-ui:latest"))
            .withNetwork(Network.SHARED)
            .dependsOn(KAFKA_CONTAINER)
            .withExposedPorts(8080)
            .waitingFor(new HttpWaitStrategy().forPort(8080))
            .withEnv("KAFKA_CLUSTERS_0_NAME", "local")
            .withEnv("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", "PLAINTEXT://" + KAFKA_CONTAINER.getNetworkAliases().get(0) + ":9092");

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        KAFKA_CONTAINER.start();

        LOG.info("Kafka started! mapped-port: {}", KAFKA_CONTAINER.getMappedPort(9093));

        KAFKA_UI_CONTAINER.start();

        LOG.info("Kafka-UI started! http://localhost:{}", KAFKA_UI_CONTAINER.getMappedPort(8080));

        ConfigurableEnvironment environment = applicationContext.getEnvironment();
        MutablePropertySources propertySources = environment.getPropertySources();
        Map<String, Object> properties = Map.of(
                "kafka.bootstrapServers", KAFKA_CONTAINER.getHost() + ":" + KAFKA_CONTAINER.getMappedPort(9093),
                "kafka.topic", TOPIC_NAME);

        propertySources.addFirst(new MapPropertySource("kafka-map", properties));

        createTopics(KAFKA_CONTAINER, TOPIC_NAME);
    }

    public static void createTopics(KafkaContainer kafka, String topic) {
        Map<String, Object> config = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(config)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        }
    }
}