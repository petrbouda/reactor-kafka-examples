package pbouda.reactor.kafka;

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
import reactor.core.publisher.Flux;

@SpringBootApplication(exclude = R2dbcAutoConfiguration.class)
public class ApplicationDBPaging implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationDBPaging.class);

    public static void main(String[] args) {
        new SpringApplicationBuilder(ApplicationDBPaging.class)
                .web(WebApplicationType.NONE)
                .bannerMode(Banner.Mode.OFF)
                .initializers(
                        new PostgresqlInitializer()
                ).run(args);
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        ConfigurableApplicationContext context = event.getApplicationContext();
        ConfigurableEnvironment env = context.getEnvironment();

        DatabaseClient databaseClient = Settings.databaseClient(env);
        Repository repository = new Repository(databaseClient);

        PushatkoDatabase pushatko = new PushatkoDatabase(repository);
        pushatko.produce(100);

        LOG.info("DB messages prepared!");

        Flux.just(" -- Just Start Simulation -- ")
                .flatMap(__ -> {
                    return repository.selectAllRaw()
                            .log()
                            .doOnNext(p -> System.out.println(p.firstname() + " " + p.lastname()));
                }, 1, 4)
                .subscribe(new AwesomeSubscriber());

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static class AwesomeSubscriber extends BaseSubscriber<Person> {

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            super.hookOnSubscribe(subscription);
        }

        @Override
        protected void hookOnNext(Person p) {
            System.out.println("FINISH! " + p.firstname() + " " + p.lastname());
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
