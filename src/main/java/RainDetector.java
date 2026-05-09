import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Properties;

public class RainDetector {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka-svc:9092");
        String weatherTopic = getEnv("WEATHER_TOPIC", "weather-readings");
        String rainTopic = getEnv("RAIN_TOPIC", "rain-alerts");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(weatherTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .filter((key, value) -> extractHumidity(value) > 70)
            .peek((key, value) -> System.out.printf("Rain alert detected for station key %s%n", key))
            .to(rainTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
        System.out.printf("RainDetector is monitoring %s and publishing to %s via %s%n", weatherTopic, rainTopic, bootstrapServers);
    }

    private static int extractHumidity(String json) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(json);
            return root.path("weather").path("humidity").asInt();
        } catch (Exception e) {
            return 0;
        }
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}