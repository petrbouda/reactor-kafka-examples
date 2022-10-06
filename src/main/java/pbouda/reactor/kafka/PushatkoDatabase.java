package pbouda.reactor.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.io.Resources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class PushatkoDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(PushatkoDatabase.class);
    private final Repository repository;

    public PushatkoDatabase(Repository repository) {
        this.repository = repository;
    }

    public void produce(int count) {
        try {
            String path = Resources.getResource("data.txt").getPath();
            List<String> lines = Files.readAllLines(Path.of(path));

            long global_i = 0;
            outer:
            while (true) {
                for (String line : lines) {
                    repository.insert(Person.ofCsv(line)).block();
                    if (++global_i >= count) {
                        break outer;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
