package pbouda.reactor.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class PostgresqlInitializer implements
        ApplicationContextInitializer<ConfigurableApplicationContext> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresqlInitializer.class);

    private static final PostgreSQLContainer<?> CONTAINER =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres"));

    @Override
    public void initialize(ConfigurableApplicationContext applicationContext) {
        CONTAINER.start();

        LOG.info("PostgreSQL started! DB_PORT: {}", CONTAINER.getMappedPort(5432));

        ConfigurableEnvironment environment = applicationContext.getEnvironment();
        MutablePropertySources propertySources = environment.getPropertySources();
        Map<String, Object> database = Map.of(
                "database.host", CONTAINER.getHost(),
                "database.port", CONTAINER.getMappedPort(5432));

        propertySources.addFirst(new MapPropertySource("postgresql-map", database));
    }
}