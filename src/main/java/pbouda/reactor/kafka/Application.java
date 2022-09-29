package pbouda.reactor.kafka;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.client.SSLMode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

@SpringBootApplication
public class Application implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        new SpringApplicationBuilder(Application.class)
                .web(WebApplicationType.REACTIVE)
                .bannerMode(Banner.Mode.OFF)
                .initializers(
                        new KafkaInitializer(),
                        new PostgresqlInitializer()
                ).run(args);
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        ConfigurableApplicationContext context = event.getApplicationContext();
        ConfigurableEnvironment env = context.getEnvironment();

        Pushatko pushatko = new Pushatko(env);
        pushatko.produce(1000);

        Config config = new Config(
                env.getRequiredProperty("kafka.topic", String.class),
                env.getRequiredProperty("kafka.bootstrapServers", String.class),
                "test-consumer",
                "earliest",
                0,
                Duration.ofSeconds(1),
                null
        );


        KafkaReceiver.create(receiverOptions(config))
                .receive()
                .map(record -> {
                    String value = record.value();
                    return value;
                })
//                .doOnNext(__ -> MESSAGE_COUNT.increment())
//                .map(messageConverter)
//                .filter(JsonConversionResult::success)
//                .flatMap(captainClient::submit)
                .subscribe(new AwesomeSubscriber());
    }

    private static class AwesomeSubscriber extends BaseSubscriber<String> {

        private static final Logger LOG = LoggerFactory.getLogger(AwesomeSubscriber.class);

        @Override
        protected void hookOnNext(String value) {
            LOG.info("Processed: {}", value);
        }

        @Override
        protected void hookOnComplete() {
            LOG.info("Processing finished!");
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            LOG.error("Processing failed!", throwable);
        }
    }

    @Bean(destroyMethod = "dispose")
    public ConnectionPool connectionFactory(
            @Value("${database.host:localhost}") String host,
            @Value("${database.port:26257}") int port,
            @Value("${database.username:root}") String username,
            @Value("${database.password:}") String password,
            @Value("${database.name:postgres}") String name,
            @Value("${database.initConnections:5}") int initConnections,
            @Value("${database.maxConnections:10}") int maxConnections,
            @Value("${database.maxIdleInSec:60}") int maxIdleInSec,
            @Value("${database.registerJmx:false}") boolean registerJmx) {

        PostgresqlConnectionConfiguration configuration =
                PostgresqlConnectionConfiguration.builder()
                        .applicationName("testapp")
                        .sslMode(SSLMode.DISABLE)
                        .host(host)
                        .port(port)
                        .username(username)
                        .password(password)
                        .database(name)
                        .build();

        ConnectionPoolConfiguration poolConfiguration =
                ConnectionPoolConfiguration.builder(new PostgresqlConnectionFactory(configuration))
                        .maxIdleTime(Duration.ofSeconds(maxIdleInSec))
                        .initialSize(initConnections)
                        .maxSize(maxConnections)
                        .name("kyc")
                        .registerJmx(registerJmx)
                        .build();

        return new ConnectionPool(poolConfiguration);
    }

    @Bean
    public DatabaseClient databaseClient(ConnectionPool connectionPool) {
        return DatabaseClient.builder()
                .connectionFactory(connectionPool)
                .build();
    }

    private static ReceiverOptions<String, String> receiverOptions(Config config) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.consumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.initialOffset());

        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(props)
                // Deferred Commits:
                // How many messages can be consumed and still waiting for the one which is not acknowledged
                // It's not precise because of message prefetching
                .maxDeferredCommits(config.deferredCommits())
                .commitInterval(config.commitInterval())
                .subscription(List.of(config.topic()));

        if (config.executor != null) {
            receiverOptions.schedulerSupplier(() -> Schedulers.fromExecutor(config.executor()));
        }

        return receiverOptions;
    }

    public record Config(
            String topic,
            String bootstrapServers,
            String consumerGroupId,
            String initialOffset,
            int deferredCommits,
            Duration commitInterval,
            ScheduledExecutorService executor) {
    }
}
