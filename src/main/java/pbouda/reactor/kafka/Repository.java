package pbouda.reactor.kafka;

import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

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

}
