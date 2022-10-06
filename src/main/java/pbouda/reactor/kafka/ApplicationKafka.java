package pbouda.reactor.kafka;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import reactor.core.publisher.BaseSubscriber;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

//@SpringBootApplication(exclude = R2dbcAutoConfiguration.class)
public class ApplicationKafka implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationKafka.class);

    public static void main(String[] args) {
        new SpringApplicationBuilder(ApplicationKafka.class)
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

        PushatkoKafka pushatko = new PushatkoKafka(env);
        pushatko.produce(10_000);

        LOG.info("Kafka messages prepared!");

        Repository repository = new Repository(Settings.databaseClient(env));

        KafkaReceiver.create(Settings.receiverOptions(env))
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
}
