import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class WeatherStation {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws InterruptedException {
        String stationId = resolveStationId();
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka-svc:9092");
        String topic = getEnv("WEATHER_TOPIC", "weather-readings");
        long sendIntervalMs = Long.parseLong(getEnv("SEND_INTERVAL_MS", "1000"));
        long sequenceNumber = 0L;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.put(ProducerConfig.LINGER_MS_CONFIG, "50");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.printf("WeatherStation %s started. Sending to %s via %s%n", stationId, topic, bootstrapServers);

            while (true) {
                sequenceNumber++;

                if (RANDOM.nextDouble() < 0.10) {
                    System.out.printf("Station %s intentionally dropped sequence %d%n", stationId, sequenceNumber);
                    Thread.sleep(sendIntervalMs);
                    continue;
                }

                String payload = buildPayload(stationId, sequenceNumber);
                long sentSequenceNumber = sequenceNumber;

                producer.send(new ProducerRecord<>(topic, stationId, payload), (metadata, exception) -> {
                    if (exception != null) {
                        System.err.printf("Failed to send station %s sequence %d: %s%n", stationId, sentSequenceNumber, exception.getMessage());
                        return;
                    }

                    System.out.printf(
                        "Station %s sent sequence %d to %s partition %d offset %d%n",
                        stationId,
                        sentSequenceNumber,
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                    );
                });

                Thread.sleep(sendIntervalMs);
            }
        }
    }

    private static String buildPayload(String stationId, long sequenceNumber) {
        ObjectNode root = OBJECT_MAPPER.createObjectNode();
        ObjectNode weather = OBJECT_MAPPER.createObjectNode();

        root.put("station_id", Long.parseLong(stationId));
        root.put("s_no", sequenceNumber);
        root.put("battery_status", randomBatteryStatus());
        root.put("status_timestamp", Instant.now().getEpochSecond());

        weather.put("humidity", RANDOM.nextInt(101));
        weather.put("temperature", 60 + RANDOM.nextInt(41));
        weather.put("wind_speed", RANDOM.nextInt(81));
        root.set("weather", weather);

        return root.toString();
    }

    private static String randomBatteryStatus() {
        int value = RANDOM.nextInt(100);
        if (value < 30) {
            return "low";
        }
        if (value < 70) {
            return "medium";
        }
        return "high";
    }

    private static String resolveStationId() {
        String configuredStationId = System.getenv("STATION_ID");
        if (configuredStationId != null && configuredStationId.matches("\\d+")) {
            return configuredStationId;
        }

        String podName = getEnv("POD_NAME", getEnv("HOSTNAME", "weather-station-1"));
        String[] segments = podName.split("-");
        String lastSegment = segments[segments.length - 1];
        if (lastSegment.matches("\\d+")) {
            return Long.toString(Long.parseLong(lastSegment) + 1);
        }

        long derivedId = Integer.toUnsignedLong(Math.abs(podName.hashCode()));
        return Long.toString(Math.max(1L, derivedId));
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}