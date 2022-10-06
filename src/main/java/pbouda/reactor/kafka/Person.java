package pbouda.reactor.kafka;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.Map;

public record Person(
        String firstname,
        String lastname,
        String city,
        String country,
        String phone,
        String political_opinion) {

    public static Person ofCsv(String line) {
        String[] parts = line.split(",");
        try {
            return new Person(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
        } catch (Exception ex) {
            System.out.println(line);
            throw ex;
        }
    }

    public static Person ofResultSet(Map<String, Object> resultSet) {
        return new Person(
                (String) resultSet.get("firstname"),
                (String) resultSet.get("lastname"),
                (String) resultSet.get("city"),
                (String) resultSet.get("country"),
                (String) resultSet.get("phone"),
                ""
        );
    }

    public static Person ofRow(Row row, RowMetadata __) {
        return new Person(
                row.get("firstname", String.class),
                row.get("lastname", String.class),
                row.get("city", String.class),
                row.get("country", String.class),
                row.get("phone", String.class),
                ""
        );
    }
}
