package pbouda.reactor.kafka;

import io.r2dbc.spi.Connection;
import org.reactivestreams.Publisher;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class Repository {

    private final DatabaseClient databaseClient;

    public static final String INSERT_PERSON = """
            INSERT INTO person (
                firstname,
                lastname,
                city,
                country,
                phone,
                political_opinion
            ) VALUES (
                :firstname,
                :lastname,
                :city,
                :country,
                :phone,
                :political_opinion
            )""";

    public static final String SELECT_ALL = """
           SELECT * FROM person""";

    public Repository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Mono<Integer> insert(Person person) {
        return databaseClient.sql(INSERT_PERSON)
                .bind("firstname", person.firstname())
                .bind("lastname", person.lastname())
                .bind("city", person.city())
                .bind("country", person.country())
                .bind("phone", person.phone())
                .bind("political_opinion", person.political_opinion())
                .fetch()
                .rowsUpdated();
    }

    /**
     * App 2022-10-05 19:39:04,701 [main] INFO  r.F.M.1 - onSubscribe(FluxMap.MapSubscriber)
     * App 2022-10-05 19:39:04,702 [main] INFO  r.F.M.1 - request(unbounded)
     * App 2022-10-05 19:39:04,706 [reactor-tcp-epoll-1] INFO  r.F.M.1 - onNext(Person[firstname=Carlene, lastname=Richmond, city=Eagleview, country=Trinidad and Tobago, phone=(898) 957-6965, political_opinion=])
     * App 2022-10-05 19:39:04,716 [reactor-tcp-epoll-1] INFO  r.F.M.1 - onNext(Person[firstname=Cheryl, lastname=Bass, city=Sunny Palms Beach, country=Guinea-Bissau, phone=(819) 117-1106, political_opinion=])
     * App 2022-10-05 19:39:04,717 [reactor-tcp-epoll-1] INFO  r.F.M.1 - onNext(Person[firstname=Bianca, lastname=McNeil, city=Americus, country=Somalia, phone=(436) 655-3194, political_opinion=])
     * App 2022-10-05 19:39:04,717 [reactor-tcp-epoll-1] INFO  r.F.M.1 - onNext(Person[firstname=Glenn, lastname=Hayden, city=Haddonfield, country=Korea, phone= North, political_opinion=])
     */
    public Flux<Person> selectAll() {
        return databaseClient.sql(SELECT_ALL)
                .fetch()
                .all()
                .map(Person::ofResultSet);
    }

    public Flux<Person> selectAllRaw() {
        Function<Connection, Publisher<Person>> dbFlow = c ->
                Flux.from(c.createStatement(SELECT_ALL).fetchSize(4).execute())
                        .flatMap(result -> result.map(Person::ofRow), 1, 4 );

        return Flux.usingWhen(databaseClient.getConnectionFactory().create(), dbFlow, Connection::close);
    }
}
