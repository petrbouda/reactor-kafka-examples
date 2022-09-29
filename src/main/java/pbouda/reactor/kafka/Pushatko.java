package pbouda.reactor.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.ConfigurableEnvironment;
import org.testcontainers.shaded.com.google.common.io.Resources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Pushatko {

    private static final Logger LOG = LoggerFactory.getLogger(Pushatko.class);

    private final String bootstrapServers;
    private final String topicName;

    public Pushatko(ConfigurableEnvironment env) {
        this.bootstrapServers = env.getRequiredProperty("kafka.bootstrapServers", String.class);
        this.topicName = env.getRequiredProperty("kafka.topic", String.class);
    }

    public void produce(int count) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties())) {
            String path = Resources.getResource("data.txt").getPath();
            List<String> lines = Files.readAllLines(Path.of(path));

            outer:
            while (true) {
                long global_i = 0;
                for (String line : lines) {
                    producer.send(new ProducerRecord<>(topicName, String.valueOf(global_i), line), producerCallback());
                    if (++global_i >= count) {
                        break outer;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Callback producerCallback() {
        return (entity, ex) -> {
            LOG.debug("Message sent: offset=" + entity.offset());
            if (ex != null) {
                LOG.error("Something wrong when sending the messages", ex);
            }
        };
    }

    private Map<String, Object> kafkaProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }
}
