package pbouda.reactor.kafka;

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
        }catch (Exception ex) {
            System.out.println(line);
            throw ex;
        }
    }
}
