package pbouda.reactor.kafka;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.client.SSLMode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Settings {

    public static ReceiverOptions<String, String> receiverOptions(ConfigurableEnvironment env) {
        String topicName = env.getRequiredProperty("kafka.topic", String.class);
        String bootstrapServers = env.getRequiredProperty("kafka.bootstrapServers", String.class);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // prefetch from Kafka
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        return ReceiverOptions.<String, String>create(props)
                // Deferred Commits:
                // How many messages can be consumed and still waiting for the one which is not acknowledged
                // It's not precise because of message prefetching
                // 1 -> 5 processed (MAX_POLL_RECORDS_CONFIG, 1)
                // 2 -> 7 processed (MAX_POLL_RECORDS_CONFIG, 1)
                // 3 -> 9 processed (MAX_POLL_RECORDS_CONFIG, 1)
                .maxDeferredCommits(5)
                // .commitBatchSize(10)
                .commitInterval(Duration.ofSeconds(1))
                // .schedulerSupplier(() -> Schedulers.fromExecutor(config.executor()))
                .subscription(List.of(topicName));
    }

    public static DatabaseClient databaseClient(ConfigurableEnvironment env) {
        ConnectionPool connectionPool = Settings.connectionFactory(
                env.getRequiredProperty("database.host", String.class),
                env.getRequiredProperty("database.port", int.class),
                env.getRequiredProperty("database.username", String.class),
                env.getRequiredProperty("database.password", String.class),
                env.getRequiredProperty("database.name", String.class));

        return DatabaseClient.builder()
                .connectionFactory(connectionPool)
                .build();
    }

    public static ConnectionPool connectionFactory(
            String host, int port, String username, String password, String name) {

        PostgresqlConnectionConfiguration configuration =
                PostgresqlConnectionConfiguration.builder()
                        .sslMode(SSLMode.DISABLE)
                        .host(host)
                        .port(port)
                        .username(username)
                        .password(password)
                        .database(name)
                        .build();

        ConnectionPoolConfiguration poolConfiguration =
                ConnectionPoolConfiguration.builder(new PostgresqlConnectionFactory(configuration))
                        .maxIdleTime(Duration.ofSeconds(10))
                        .initialSize(1)
                        .maxSize(10)
                        .build();

        return new ConnectionPool(poolConfiguration);
    }
}
