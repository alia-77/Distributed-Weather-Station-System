import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CentralStation {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka-svc:9092");
        String weatherTopic = getEnv("WEATHER_TOPIC", "weather-readings");
        String dbUrl = getEnv("DB_URL", "jdbc:postgresql://postgres-svc:5432/weather_db");
        String dbUser = getEnv("DB_USER", "weather_user");
        String dbPassword = getEnv("DB_PASSWORD", "weather_password");
        int batchSize = Integer.parseInt(getEnv("BATCH_SIZE", "5000"));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             Connection connection = connectWithRetry(dbUrl, dbUser, dbPassword)) {
            consumer.subscribe(Collections.singletonList(weatherTopic));
            connection.setAutoCommit(false);
            initializeSchema(connection);

            String sql = "INSERT INTO weather_readings (station_id, sequence_number, battery_status, status_timestamp, humidity, temperature, wind_speed) VALUES (?, ?, ?, ?, ?, ?, ?)";
            int pendingBatchEntries = 0;

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                System.out.printf("CentralStation started. Consuming %s and writing to %s%n", weatherTopic, dbUrl);

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            JsonNode root = OBJECT_MAPPER.readTree(record.value());
                            JsonNode weather = root.path("weather");

                            statement.setLong(1, root.path("station_id").asLong());
                            statement.setLong(2, root.path("s_no").asLong());
                            statement.setString(3, root.path("battery_status").asText());
                            statement.setLong(4, root.path("status_timestamp").asLong());
                            statement.setInt(5, weather.path("humidity").asInt());
                            statement.setInt(6, weather.path("temperature").asInt());
                            statement.setInt(7, weather.path("wind_speed").asInt());
                            statement.addBatch();
                            pendingBatchEntries++;
                        } catch (Exception e) {
                            System.err.printf("Skipping invalid payload: %s%n", e.getMessage());
                        }
                    }

                    if (pendingBatchEntries >= batchSize) {
                        statement.executeBatch();
                        connection.commit();
                        consumer.commitSync();
                        System.out.printf("Inserted batch of %d records.%n", pendingBatchEntries);
                        pendingBatchEntries = 0;
                    }
                }
            }
        }
    }

    private static Connection connectWithRetry(String dbUrl, String dbUser, String dbPassword) throws InterruptedException {
        while (true) {
            try {
                return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            } catch (Exception exception) {
                System.err.printf("Database not ready yet (%s). Retrying in 5 seconds...%n", exception.getMessage());
                Thread.sleep(5000);
            }
        }
    }

    private static void initializeSchema(Connection connection) throws Exception {
        String schemaSql = """
            CREATE TABLE IF NOT EXISTS weather_readings (
                id BIGSERIAL PRIMARY KEY,
                station_id BIGINT NOT NULL,
                sequence_number BIGINT NOT NULL,
                battery_status VARCHAR(10) NOT NULL,
                status_timestamp BIGINT NOT NULL,
                humidity INT NOT NULL,
                temperature INT NOT NULL,
                wind_speed INT NOT NULL
            )
            """;

        try (Statement statement = connection.createStatement()) {
            statement.execute(schemaSql);
            connection.commit();
        }
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}