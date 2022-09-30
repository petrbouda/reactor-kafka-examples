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
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication(exclude = R2dbcAutoConfiguration.class)
public class Application implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        new SpringApplicationBuilder(Application.class)
                .web(WebApplicationType.NONE)
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
        pushatko.produce(10_000);

        LOG.info("Kafka messages prepared!");

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
                .receive()
                .flatMap(record -> {
                    LOG.info("Start processing: offset={}", record.receiverOffset().offset());
                    Person person = Person.ofCsv(record.value());

                    return repository.insert(person)
                            .delayElement(Duration.ofMillis(500))
                            .map(__ -> record);
                }, 1) // - Flatmap has concurrency 1 that means that we are able to use only one DB connection.
                // })
                // - Generation of inners and subscription: this operator is eagerly subscribing to its inners.
                // - Ordering of the flattened values: this operator does not necessarily preserve original ordering,
                //      as inner element are flattened as they arrive.
                // - Interleaving: this operator lets values from different inners interleave (similar to merging the inner sequences).
                //
                // Look at the logs when we change parameters in FlatMap
                // concurrency – the maximum number of in-flight inner sequences
                // prefetch – the maximum in-flight elements from each inner Publisher sequence
                .subscribe(new AwesomeSubscriber());
    }

    private static class AwesomeSubscriber extends BaseSubscriber<ReceiverRecord<String, String>> {

        private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor();

        /**
         * The ACKs will be released from the lowest to the highest offsets
         * - that means that the Deferred Commit can be immediately committed and new record processed.
         */
        private static final Queue<ReceiverOffset> DELAYED_ACKS_QUEUE = new ConcurrentLinkedQueue<>();

        /**
         * The ACKs will be released from the highest to the lowest offsets
         * - that means that the Deferred Commit needs to wait for the lowest one to be released
         * (to fill the gab in the row).
         */
        private static final Stack<ReceiverOffset> DELAYED_ACKS_STACK = new Stack<>();

        private static final Logger LOG = LoggerFactory.getLogger(AwesomeSubscriber.class);

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            super.hookOnSubscribe(subscription);

            EXECUTOR.scheduleAtFixedRate(() -> {
                ReceiverOffset offset;
                while ((offset = DELAYED_ACKS_QUEUE.poll()) != null) {
//                while ((offset = DELAYED_ACKS_STACK.pop()) != null) {
                    offset.acknowledge();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                    LOG.info("RELEASE: offset acknowledged: {}", offset);
                }
            }, 30, 30, TimeUnit.SECONDS);
        }

        @Override
        protected void hookOnNext(ReceiverRecord<String, String> record) {
            ReceiverOffset receiverOffset = record.receiverOffset();
            LOG.info("Processed: offset={}", receiverOffset.offset());

            int key = Integer.parseInt(record.key());
            if (key % 2 == 0) {
                receiverOffset.acknowledge();
            } else {
//                DELAYED_ACKS_STACK.push(receiverOffset);
                 DELAYED_ACKS_QUEUE.offer(receiverOffset);
            }
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
}
