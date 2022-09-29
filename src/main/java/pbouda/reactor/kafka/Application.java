package pbouda.reactor.kafka;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.client.SSLMode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Hooks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication(exclude = R2dbcAutoConfiguration.class)
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

        ConnectionPool connectionPool = connectionFactory(
                env.getRequiredProperty("database.host", String.class),
                env.getRequiredProperty("database.port", int.class),
                env.getRequiredProperty("database.username", String.class),
                env.getRequiredProperty("database.password", String.class),
                env.getRequiredProperty("database.name", String.class));

        DatabaseClient databaseClient = DatabaseClient.builder()
                .connectionFactory(connectionPool)
                .build();

        Repository repository = new Repository(databaseClient);

        ReceiverOptions<String, String> receiverOptions = receiverOptions(
                env.getRequiredProperty("kafka.topic", String.class),
                env.getRequiredProperty("kafka.bootstrapServers", String.class));

        KafkaReceiver.create(receiverOptions)
                .receive(1)
                .flatMap(record -> {
                    LOG.info("Start processing: offset={}", record.receiverOffset().offset());
                    Person person = Person.ofCsv(record.value());
                    return repository.insert(person)
                            .map(__ -> record.receiverOffset());
                // }, 1) - Flatmap has concurrency 1 that means that we are able to use only one DB connection.
                })
                .subscribe(new AwesomeSubscriber());
    }

    private static class AwesomeSubscriber extends BaseSubscriber<ReceiverOffset> {

        private static final Logger LOG = LoggerFactory.getLogger(AwesomeSubscriber.class);

//        @Override
//        protected void hookOnSubscribe(Subscription subscription) {
//            subscription.request(1);
//        }

        @Override
        protected void hookOnNext(ReceiverOffset offset) {
            LOG.info("Processed: offset={}", offset.offset());
            offset.acknowledge();
//            request(1);
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

    private static ConnectionPool connectionFactory(
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

    private static ReceiverOptions<String, String> receiverOptions(String topicName, String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return ReceiverOptions.<String, String>create(props)
                // Deferred Commits:
                // How many messages can be consumed and still waiting for the one which is not acknowledged
                // It's not precise because of message prefetching
                .maxDeferredCommits(0)
                .commitInterval(Duration.ofSeconds(1))
                // .schedulerSupplier(() -> Schedulers.fromExecutor(config.executor()))
                .subscription(List.of(topicName));
    }
}
